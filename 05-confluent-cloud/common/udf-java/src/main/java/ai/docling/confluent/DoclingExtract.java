// Copyright IBM Corp. 2026
// SPDX-License-Identifier: Apache-2.0
package ai.docling.confluent;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Structured extraction with Docling, from inside a Flink statement.
 *
 * <pre>
 *   dc_extract(docling_connection, artifact_connection, url, extraction_target)
 *       -&gt;  Docling's ExtractionDocumentResult, as a JSON STRING
 * </pre>
 *
 * The extraction API of docling-project/docling#4218: an
 * {@code ExtractionTarget} (a JSON Schema, a tagged template, instructions)
 * and one {@code http} source in, one {@code ExtractionDocumentResult} out —
 * {@code source_index, source_uri, filename, status, errors[], items[]}, one
 * item per page with {@code extracted_data}, {@code validation_status} and the
 * model's telemetry. That result is returned <em>as Docling wrote it</em>, so
 * whatever reads it downstream reads Docling's contract, not this class's.
 *
 * <h2>Why not docling-java</h2>
 *
 * {@link DoclingConvert} uses the official Java client; the pinned release
 * predates the extraction API. Three plain HTTP calls are all it takes —
 * submit, poll, fetch the result — so this is {@code java.net.http} and
 * Jackson, both already on the classpath, rather than a client fork.
 *
 * <h2>The calls</h2>
 *
 * <pre>
 *   POST {endpoint}/v1/extract/source/async   {extraction_target, sources, target}
 *   GET  {endpoint}/v1/status/poll/{task_id}  until success or failure
 *   GET  {endpoint}/v1/result/{task_id}       documents[0]
 *   GET  documents[0].artifacts[0].uri        the result itself, on S3 (presigned)
 * </pre>
 *
 * Docling for IBM watsonx answers {@code target: presigned_url} only (an
 * {@code inbody} target is a 422), so the result lives on the service's
 * artifact store and the fourth call needs the second connection — exactly as
 * for {@link DoclingConvert}. A self-hosted docling-serve answers in the body
 * with {@code resultTarget = "inbody"}; both shapes are handled.
 *
 * <p>No {@code options} are sent. Every field defaults server-side, and one of
 * the defaults the Python client fills in — {@code page_range: [1, maxint]} —
 * makes the SaaS fail the task outright.
 *
 * <h2>Retries</h2>
 *
 * The SaaS's model server times out under load: a task fails with
 * {@code retryable: true}, or a page comes back with an error and no model
 * answer at all (no {@code inference_metadata}) while its neighbour extracted
 * fine. Both
 * are asked again, up to {@link #MAX_ATTEMPTS} times and within
 * {@link #DEADLINE}; whatever the last attempt answered is what is returned. A
 * page the model answered <em>wrongly</em> is not retried — that is quality,
 * and the pipeline's review queue is where it goes.
 *
 * <h2>Failures are values</h2>
 *
 * Anything that goes wrong is a row, never an exception — a statement that
 * stops on one bad invoice stops the clock for all of them:
 *
 * <pre>
 *   {"source_index":0,"source_uri":"...","filename":"...","status":"failure",
 *    "errors":[{"component_type":"udf","module_name":"DoclingExtract","error_message":"..."}],
 *    "items":[]}
 * </pre>
 *
 * <h2>Asynchronous, because it is slow</h2>
 *
 * An extraction takes seconds per page. As a plain {@code ScalarFunction} —
 * the first version — the calls of a batch ran one after another and Confluent
 * Cloud's timeout applies to the batch: a day of 25 invoices took twenty
 * minutes to produce five rows, the rest of the time spent in batches cut off
 * and run again (and billed again). An {@link AsyncScalarFunction} hands Flink
 * a future instead: several extractions are in flight at once, results still
 * come out in input order — the simulated clock depends on that — and the
 * timeout is per call.
 *
 * <p>That timeout is about <b>60 seconds</b> on Confluent Cloud (measured: an
 * ad hoc call on a two-page invoice failed after ~65 s; the documentation gives
 * no number), it is not configurable ({@code table.exec.async-scalar.*} is
 * refused as a statement option), and a call that hits it fails the whole
 * statement, not the row. So {@link #DEADLINE} is well under it and counts
 * from the moment Flink hands over the record, queueing included; every HTTP
 * timeout is capped by what is left of it. This always answers — with a
 * failure envelope if need be — before Flink would give up.
 *
 * <p>The HTTP calls stay blocking, on {@link #THREADS} threads of this
 * function's own, so the code reads top to bottom. Few, on purpose: Docling for
 * IBM watsonx answers slower with more extractions at once, and its model
 * server's own 20 s timeout then starts dropping pages.
 */
public class DoclingExtract extends AsyncScalarFunction {

  private static final long serialVersionUID = 1L;

  /** Total time one record may take, from eval() — queueing and all attempts — under the ~60 s limit. */
  static final Duration DEADLINE = Duration.ofSeconds(45);
  /** Extractions running at once, per function instance. */
  static final int THREADS = 4;
  static final int MAX_ATTEMPTS = 3;
  static final Duration POLL_INTERVAL = Duration.ofSeconds(2);
  static final Duration HTTP_TIMEOUT = Duration.ofSeconds(60);

  private transient ObjectMapper mapper;
  private transient HttpClient http;
  private transient FunctionContext context;
  private transient ExecutorService pool;
  /** Test seam: the poll interval, shortened by the tests. */
  transient Duration pollInterval = POLL_INTERVAL;

  @Override
  public void open(FunctionContext context) throws Exception {
    this.context = context;
    init();
  }

  void init() {
    if (mapper == null) {
      mapper = new ObjectMapper();
    }
    if (http == null) {
      http = HttpClient.newBuilder()
          .connectTimeout(Duration.ofSeconds(15))
          .followRedirects(HttpClient.Redirect.NORMAL)
          .build();
    }
    if (pollInterval == null) {
      pollInterval = POLL_INTERVAL;
    }
    if (pool == null) {
      pool = Executors.newFixedThreadPool(THREADS, runnable -> {
        Thread thread = new Thread(runnable, "docling-extract");
        thread.setDaemon(true);
        return thread;
      });
    }
  }

  @Override
  public void close() throws Exception {
    if (pool != null) {
      pool.shutdownNow();
      pool = null;
    }
  }

  /**
   * @param doclingConn   the Flink CONNECTION naming the Docling service
   * @param downloadConn  the CONNECTION naming the artifact host, or NULL
   * @param url           a public http(s) URL of the document
   * @param targetJson    an ExtractionTarget, as JSON
   */
  public void eval(CompletableFuture<String> result, String doclingConn, String downloadConn, String url,
      String targetJson) {
    eval(result, doclingConn, downloadConn, url, targetJson, "presigned_url");
  }

  /** As above, with the result target: "presigned_url" (the SaaS) or "inbody". */
  public void eval(CompletableFuture<String> result, String doclingConn, String downloadConn, String url,
      String targetJson, String resultTarget) {
    init();
    Instant deadline = Instant.now().plus(DEADLINE);
    try {
      pool.execute(() -> {
        try {
          result.complete(call(doclingConn, downloadConn, url, targetJson, resultTarget, deadline));
        } catch (Throwable throwable) {  // NOPMD — the future must always complete with a value
          result.complete(failure(url, throwable.getClass().getSimpleName() + ": " + throwable.getMessage()));
        }
      });
    } catch (RuntimeException rejected) {  // the pool is shut down: the function is closing
      result.complete(failure(url, "the function is closing: " + rejected.getMessage()));
    }
  }

  /** One extraction, on the calling thread. Never throws. */
  String call(String doclingConn, String downloadConn, String url, String targetJson, String resultTarget,
      Instant deadline) {
    try {
      String endpoint = jobParameter(doclingConn, "endpoint");
      if (endpoint == null || endpoint.isBlank()) {
        return failure(url, "connection '" + doclingConn + "' has no endpoint. Is the function bound "
            + "to it with USING CONNECTIONS, and is that the connection's name?");
      }
      String downloadEndpoint = downloadConn == null || downloadConn.isBlank()
          ? null : jobParameter(downloadConn, "endpoint");
      return extract(endpoint, jobParameter(doclingConn, "token"), downloadEndpoint, url, targetJson,
          resultTarget, deadline);
    } catch (Exception exception) {  // NOPMD — a bad row must not stop the stream
      return failure(url, exception.getClass().getSimpleName() + ": " + exception.getMessage());
    }
  }

  /** The whole call, from resolved endpoints — what the tests drive. Never throws. */
  String extract(String endpoint, String token, String downloadEndpoint, String url,
      String targetJson, String resultTarget) {
    return extract(endpoint, token, downloadEndpoint, url, targetJson, resultTarget, Instant.now().plus(DEADLINE));
  }

  String extract(String endpoint, String token, String downloadEndpoint, String url,
      String targetJson, String resultTarget, Instant deadline) {
    init();
    if (url == null || url.isBlank()) {
      return failure(url, "no url");
    }
    JsonNode target;
    try {
      target = mapper.readTree(targetJson == null ? "" : targetJson);
    } catch (IOException exception) {
      return failure(url, "extraction_target is not valid JSON: " + exception.getMessage());
    }
    if (target == null || !target.isObject()) {
      return failure(url, "extraction_target must be a JSON object");
    }

    String last = null;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        Attempt result = once(endpoint, token, downloadEndpoint, url, target,
            resultTarget == null || resultTarget.isBlank() ? "presigned_url" : resultTarget, deadline);
        last = result.envelope;
        if (!result.retryable) {
          return last;
        }
      } catch (Exception exception) {  // NOPMD
        last = failure(url, exception.getClass().getSimpleName() + ": " + exception.getMessage());
      }
      if (Instant.now().plus(Duration.ofSeconds(15)).isAfter(deadline)) {
        break;  // not enough left for another attempt to finish
      }
      sleep(pollInterval.multipliedBy(attempt));
    }
    return last;
  }

  private record Attempt(String envelope, boolean retryable) { }

  private Attempt once(String endpoint, String token, String downloadEndpoint, String url,
      JsonNode target, String resultTarget, Instant deadline) throws IOException, InterruptedException {
    String base = endpoint.replaceAll("/+$", "");

    ObjectNode body = mapper.createObjectNode();
    body.set("extraction_target", target);
    ArrayNode sources = body.putArray("sources");
    sources.addObject().put("kind", "http").put("url", url);
    body.putObject("target").put("kind", resultTarget);

    JsonNode submitted = call(base + "/v1/extract/source/async", token, mapper.writeValueAsString(body), deadline);
    String taskId = submitted.path("task_id").asText("");
    if (taskId.isEmpty()) {
      return new Attempt(failure(url, "the service accepted no task: " + submitted), false);
    }

    JsonNode status = submitted;
    while (!isDone(status.path("task_status").asText(""))) {
      if (Instant.now().isAfter(deadline)) {
        return new Attempt(failure(url, "the extraction did not finish within " + DEADLINE.toSeconds()
            + " s (task " + taskId + ")"), false);
      }
      sleep(pollInterval);
      status = call(base + "/v1/status/poll/" + taskId, token, null, deadline);
    }
    if (!"success".equals(status.path("task_status").asText())) {
      JsonNode fail = status.path("failure");
      String message = status.path("error_message").asText(fail.path("message").asText("task failed"));
      return new Attempt(failure(url, "task " + taskId + " " + status.path("task_status").asText()
          + ": " + message), fail.path("retryable").asBoolean(false));
    }

    JsonNode response = call(base + "/v1/result/" + taskId, token, null, deadline);
    JsonNode documents = response.path("documents");
    if (!documents.isArray() || documents.isEmpty()) {
      return new Attempt(failure(url, "the extraction returned no documents"), false);
    }
    JsonNode document = documents.get(0);
    JsonNode envelope;
    if (document.has("items")) {
      envelope = document;  // in the body: already an ExtractionDocumentResult
    } else {
      JsonNode artifacts = document.path("artifacts");
      if (!artifacts.isArray() || artifacts.isEmpty()) {
        return new Attempt(failure(url, "document status " + document.path("status").asText("?")
            + " and no artifacts: " + document.path("errors")), false);
      }
      URI artifact = URI.create(artifacts.get(0).path("uri").asText());
      requireReachable(downloadEndpoint, artifact);
      envelope = mapper.readTree(get(artifact, deadline));
    }
    return new Attempt(mapper.writeValueAsString(envelope), hasInferenceFailure(envelope));
  }

  static boolean isDone(String taskStatus) {
    return "success".equals(taskStatus) || "failure".equals(taskStatus) || "partial_success".equals(taskStatus)
        || "skipped".equals(taskStatus);
  }

  /**
   * A page the model never answered: errors, and no inference metadata. Not
   * the error category — Docling files every item error, a schema violation
   * included, under {@code inference_failure}, and a wrong answer is not worth
   * paying for twice.
   */
  static boolean hasInferenceFailure(JsonNode envelope) {
    for (JsonNode item : envelope.path("items")) {
      JsonNode metadata = item.path("inference_metadata");
      if (item.path("errors").size() > 0 && (metadata.isMissingNode() || metadata.isNull())) {
        return true;
      }
    }
    return false;
  }

  // --- HTTP ---------------------------------------------------------------------

  /** What is left of the deadline, capped at {@link #HTTP_TIMEOUT}; at least a second. */
  static Duration remaining(Instant deadline) {
    Duration left = Duration.between(Instant.now(), deadline);
    if (left.compareTo(Duration.ofSeconds(1)) < 0) {
      return Duration.ofSeconds(1);
    }
    return left.compareTo(HTTP_TIMEOUT) > 0 ? HTTP_TIMEOUT : left;
  }

  private JsonNode call(String uri, String token, String jsonBody, Instant deadline)
      throws IOException, InterruptedException {
    if (Instant.now().isAfter(deadline)) {
      throw new IOException("out of time before " + URI.create(uri).getPath());
    }
    HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(uri)).timeout(remaining(deadline))
        .header("Accept", "application/json");
    if (token != null && !token.isBlank()) {
      // Docling for IBM watsonx reads the key from X-Api-Key, not a bearer token.
      request.header("X-Api-Key", token);
    }
    if (jsonBody != null) {
      request.header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8));
    } else {
      request.GET();
    }
    HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() / 100 != 2) {
      String text = response.body() == null ? "" : response.body();
      throw new IOException(request.build().method() + " " + URI.create(uri).getPath() + " answered HTTP "
          + response.statusCode() + ": " + text.substring(0, Math.min(400, text.length())));
    }
    return mapper.readTree(response.body());
  }

  private String get(URI uri, Instant deadline) throws IOException, InterruptedException {
    HttpResponse<String> response = http.send(
        HttpRequest.newBuilder(uri).timeout(remaining(deadline)).GET().build(), HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() / 100 != 2) {
      throw new IOException("downloading the result answered HTTP " + response.statusCode());
    }
    return response.body();
  }

  private static void requireReachable(String downloadEndpoint, URI uri) {
    if (downloadEndpoint == null || downloadEndpoint.isBlank()) {
      return;  // nothing to check; an unbound host fails the GET, and the row says so
    }
    URI bound = URI.create(downloadEndpoint);
    if (bound.getHost() == null || !bound.getHost().equalsIgnoreCase(uri.getHost())) {
      throw new IllegalStateException("the result is on " + uri.getHost() + ", but the artifact connection"
          + " points at " + bound.getHost() + " — a UDF may only reach hosts it is bound to");
    }
  }

  private static void sleep(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  // --- helpers ------------------------------------------------------------------

  private String jobParameter(String connection, String field) {
    if (connection == null || connection.isBlank() || context == null) {
      return null;
    }
    return context.getJobParameter(connection + "." + field, null);
  }

  /** What Docling reports as the file name: the URL path's last segment. */
  static String filename(String url) {
    if (url == null) {
      return "";
    }
    try {
      String path = URI.create(url).getPath();
      String name = path == null ? "" : path.substring(path.lastIndexOf('/') + 1);
      return URLDecoder.decode(name, StandardCharsets.UTF_8);
    } catch (IllegalArgumentException exception) {
      return url;
    }
  }

  String failure(String url, String message) {
    init();
    ObjectNode out = mapper.createObjectNode();
    out.put("source_index", 0);
    out.put("source_uri", url == null ? "" : url);
    out.put("filename", filename(url));
    out.put("status", "failure");
    out.putArray("errors").addObject()
        .put("component_type", "udf")
        .put("module_name", "DoclingExtract")
        .put("error_message", message == null ? "unknown" : message);
    out.putArray("items");
    try {
      return mapper.writeValueAsString(out);
    } catch (IOException exception) {
      return "{\"status\":\"failure\",\"errors\":[],\"items\":[]}";
    }
  }
}
