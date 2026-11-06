package ai.docling.confluent;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Convert one document with Docling, from inside a Flink statement.
 *
 * <pre>
 *   dc_convert(docling_connection, artifact_connection, url [, format])
 *       -&gt;  a JSON envelope, as a STRING
 * </pre>
 *
 * <h2>Why this is Java</h2>
 *
 * A platform constraint, not a preference. A Confluent Cloud Python UDF cannot
 * open an HTTPS socket at all — its interpreter has no {@code ssl} module, so
 * {@code urlopen} answers "unknown url type: https" — and binding a Python
 * function to a {@code CONNECTION} is accepted by {@code CREATE FUNCTION} and
 * then fails at runtime. External connectivity is Java-only.
 * {@code docs/05-confluent-cloud/LIMITS.md} records how that was established, on a live account.
 *
 * <h2>The calls</h2>
 *
 * <pre>
 *   POST {endpoint}/v1/convert/source/async   {sources: [{kind: http, url}], options: {to_formats}}
 *   GET  {endpoint}/v1/status/poll/{task_id}  until success or failure
 *   GET  {endpoint}/v1/result/{task_id}       documents[0], or document (in the body)
 *   GET  documents[0].artifacts[0].uri        the converted document, on S3 (presigned)
 * </pre>
 *
 * Docling for IBM watsonx answers with a <em>presigned URL</em> on Amazon S3,
 * not with the document — there is no {@code inbody} target to ask for — so the
 * last call goes to a host that is not the service's, and a UDF may only reach
 * hosts it is bound to: hence the second connection, which carries no
 * credential (a presigned URL is its own). A self-hosted docling-serve answers
 * in the body; both shapes are handled, and no {@code target} is sent, so each
 * service answers in its default one.
 *
 * <p>Plain {@code java.net.http} and Jackson, like {@link DoclingExtract}:
 * every HTTP timeout here is capped by what is left of the deadline below,
 * which a client with its own polling loop does not let you do.
 *
 * <h2>The connections</h2>
 *
 * A UDF never holds a credential. Confluent Cloud injects a bound
 * {@code CONNECTION}'s endpoint and token as job parameters named after the
 * connection — {@code <name>.endpoint}, {@code <name>.token} — and only those
 * two fields. The names are arguments rather than constants, because every name
 * in this repo is derived from {@code NS}.
 *
 * <h2>Asynchronous, with a deadline</h2>
 *
 * The first version was a synchronous {@code ScalarFunction}. Confluent Cloud
 * applies its timeout to the <em>batch</em> a record belongs to, so the calls
 * of a batch ran one after another, a batch that overran was cut off and run
 * again — and every conversion in it billed again. Example 04 measured the same
 * thing for extraction: five rows in twenty minutes.
 *
 * <p>As an {@link AsyncScalarFunction}, several conversions are in flight at
 * once ({@link #THREADS}), results still come out in input order, and the
 * timeout is per call. That timeout is about <b>60 seconds</b>, not
 * configurable, and a call that passes it fails the whole statement, not the
 * row (see {@code docs/05-confluent-cloud/LIMITS.md}). So {@link #DEADLINE} counts from the moment
 * Flink hands over the record, queueing included, and this always answers
 * before it — with a failure envelope if that is what it has. A conversion on
 * Docling for IBM watsonx takes one to four seconds for a ten-page paper
 * (2026-10-06), so the deadline is for the outliers.
 *
 * <h2>What it returns</h2>
 *
 * One JSON object, always, so that a failed conversion is a row you can see
 * rather than a statement that stops:
 *
 * <pre>
 *   {"status":"success","url":"...","filename":"...","source_uri":"...","format":"markdown",
 *    "content":"# ...","processing_time":2.2,"elapsed_ms":3400,"attempts":1,"confidence":"good"}
 *   {"status":"failure","url":"...","error":"...","elapsed_ms":45000,"attempts":3}
 * </pre>
 *
 * Turning that into chunk records is {@code dc_chunks}, in the Python
 * artifact, so the chunk shape stays in one language and one file.
 */
public class DoclingConvert extends AsyncScalarFunction {

  private static final long serialVersionUID = 1L;

  /** Total time one record may take, from eval() — queueing and all attempts — under the ~60 s limit. */
  static final Duration DEADLINE = Duration.ofSeconds(45);
  /** Conversions running at once, per function instance. */
  static final int THREADS = 4;
  static final int MAX_ATTEMPTS = 3;
  static final Duration POLL_INTERVAL = Duration.ofSeconds(1);
  static final Duration HTTP_TIMEOUT = Duration.ofSeconds(30);

  private transient ObjectMapper mapper;
  private transient HttpClient http;
  private transient FunctionContext context;
  private transient ExecutorService pool;
  /** Test seam: the poll interval, shortened by the tests. */
  transient Duration pollInterval = POLL_INTERVAL;
  /** Test seam: the deadline, shortened by the tests. */
  transient Duration deadline = DEADLINE;

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
          .connectTimeout(Duration.ofSeconds(10))
          .followRedirects(HttpClient.Redirect.NORMAL)
          .build();
    }
    if (pollInterval == null) {
      pollInterval = POLL_INTERVAL;
    }
    if (deadline == null) {
      deadline = DEADLINE;
    }
    if (pool == null) {
      pool = Executors.newFixedThreadPool(THREADS, runnable -> {
        Thread thread = new Thread(runnable, "docling-convert");
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
   * @param downloadConn  the CONNECTION naming the artifact host, or NULL when
   *                      the service answers in the body
   * @param url           a public http(s) URL of the document to convert
   * @param format        "md", "json", "html", "text" or "doctags"
   */
  public void eval(CompletableFuture<String> result, String doclingConn, String downloadConn, String url,
      String format) {
    init();
    Instant until = Instant.now().plus(deadline);
    try {
      pool.execute(() -> {
        try {
          result.complete(call(doclingConn, downloadConn, url, format, until));
        } catch (Throwable throwable) {  // NOPMD — the future must always complete with a value
          result.complete(failure(url, throwable.getClass().getSimpleName() + ": " + throwable.getMessage(),
              until, 0));
        }
      });
    } catch (RuntimeException rejected) {  // the pool is shut down: the function is closing
      result.complete(failure(url, "the function is closing: " + rejected.getMessage(), until, 0));
    }
  }

  /** Markdown. */
  public void eval(CompletableFuture<String> result, String doclingConn, String downloadConn, String url) {
    eval(result, doclingConn, downloadConn, url, "md");
  }

  /** One conversion, on the calling thread, from connection names. Never throws. */
  String call(String doclingConn, String downloadConn, String url, String format, Instant until) {
    String endpoint = jobParameter(doclingConn, "endpoint");
    if (endpoint == null || endpoint.isBlank()) {
      return failure(url, "connection '" + doclingConn + "' has no endpoint. Is the function bound "
          + "to it with USING CONNECTIONS, and is that the connection's name?", until, 0);
    }
    String downloadEndpoint = downloadConn == null || downloadConn.isBlank()
        ? null : jobParameter(downloadConn, "endpoint");
    return convert(endpoint, jobParameter(doclingConn, "token"), downloadEndpoint, url, format, until);
  }

  /** The whole call, from resolved endpoints — what the tests drive. Never throws. */
  String convert(String endpoint, String token, String downloadEndpoint, String url, String format) {
    init();
    return convert(endpoint, token, downloadEndpoint, url, format, Instant.now().plus(deadline));
  }

  String convert(String endpoint, String token, String downloadEndpoint, String url, String format,
      Instant until) {
    init();
    if (url == null || url.isBlank()) {
      return failure(url, "no url", until, 0);
    }
    String toFormat;
    try {
      toFormat = toFormat(format);
    } catch (IllegalArgumentException exception) {
      return failure(url, exception.getMessage(), until, 0);
    }

    String last = null;
    int attempt = 1;
    for (; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        Attempt result = once(endpoint, token, downloadEndpoint, url, toFormat, until, attempt);
        last = result.envelope;
        if (!result.retryable) {
          return last;
        }
      } catch (Exception exception) {  // NOPMD — a bad row must not stop the stream
        last = failure(url, exception.getClass().getSimpleName() + ": " + exception.getMessage(), until, attempt);
        // A 4xx is the request's fault — a URL the service will not fetch, a
        // format it does not offer — and asking again only bills again.
        if (exception instanceof HttpStatusException status && status.code / 100 == 4 && status.code != 429) {
          return last;
        }
      }
      if (Instant.now().plus(Duration.ofSeconds(10)).isAfter(until)) {
        break;  // not enough left for another attempt to finish
      }
      sleep(pollInterval.multipliedBy(attempt));
    }
    return last;
  }

  private record Attempt(String envelope, boolean retryable) { }

  /** An HTTP answer outside 2xx, with its status kept so the retry can tell 4xx from 5xx. */
  static final class HttpStatusException extends IOException {
    private static final long serialVersionUID = 1L;
    final int code;

    HttpStatusException(String message, int code) {
      super(message);
      this.code = code;
    }
  }

  private Attempt once(String endpoint, String token, String downloadEndpoint, String url, String toFormat,
      Instant until, int attempt) throws IOException, InterruptedException {
    String base = endpoint.replaceAll("/+$", "");

    ObjectNode body = mapper.createObjectNode();
    body.putArray("sources").addObject().put("kind", "http").put("url", url);
    body.putObject("options").putArray("to_formats").add(toFormat);

    JsonNode submitted = request(base + "/v1/convert/source/async", token, mapper.writeValueAsString(body), until);
    String taskId = submitted.path("task_id").asText("");
    if (taskId.isEmpty()) {
      return new Attempt(failure(url, "the service accepted no task: " + submitted, until, attempt), false);
    }

    JsonNode status = submitted;
    while (!isDone(status.path("task_status").asText(""))) {
      if (Instant.now().isAfter(until)) {
        return new Attempt(failure(url, "the conversion did not finish within " + deadline.toSeconds()
            + " s (task " + taskId + ")", until, attempt), false);
      }
      sleep(pollInterval);
      status = request(base + "/v1/status/poll/" + taskId, token, null, until);
    }
    String taskStatus = status.path("task_status").asText();
    if (!"success".equals(taskStatus) && !"partial_success".equals(taskStatus)) {
      JsonNode fail = status.path("failure");
      String message = status.path("error_message").asText(fail.path("message").asText("task failed"));
      return new Attempt(failure(url, "task " + taskId + " " + taskStatus + ": " + message, until, attempt),
          fail.path("retryable").asBoolean(false));
    }

    JsonNode response = request(base + "/v1/result/" + taskId, token, null, until);
    double processingTime = response.path("processing_time").asDouble(0.0);

    // In the body — a self-hosted docling-serve: {"document": {"md_content": ...}}.
    JsonNode inBody = response.path("document");
    if (inBody.isObject()) {
      String content = inBody.path(toFormat + "_content").asText(inBody.path("md_content").asText(""));
      return new Attempt(success(url, inBody.path("filename").asText(""), url, formatName(toFormat), content,
          processingTime, null, until, attempt), false);
    }

    // Presigned — Docling for IBM watsonx: {"documents": [{"artifacts": [{"uri": ...}]}]}.
    JsonNode documents = response.path("documents");
    if (!documents.isArray() || documents.isEmpty()) {
      return new Attempt(failure(url, "the conversion returned no documents", until, attempt), false);
    }
    JsonNode document = documents.get(0);
    JsonNode artifacts = document.path("artifacts");
    if (!artifacts.isArray() || artifacts.isEmpty()) {
      return new Attempt(failure(url, "document status " + document.path("status").asText("?")
          + " and no artifacts: " + document.path("errors"), until, attempt), false);
    }
    JsonNode artifact = artifacts.get(0);
    URI uri = URI.create(artifact.path("uri").asText());
    requireReachable(downloadEndpoint, uri);
    String content = download(uri, until);
    String confidence = document.path("confidence").path("mean_grade").asText(null);
    return new Attempt(success(url, document.path("filename").asText(""),
        document.path("source_uri").asText(url), artifact.path("artifact_type").asText(formatName(toFormat)),
        content, processingTime, confidence, until, attempt), false);
  }

  static boolean isDone(String taskStatus) {
    return "success".equals(taskStatus) || "failure".equals(taskStatus) || "partial_success".equals(taskStatus)
        || "skipped".equals(taskStatus);
  }

  // --- HTTP ---------------------------------------------------------------------

  /** What is left of the deadline, capped at {@link #HTTP_TIMEOUT}; at least a second. */
  static Duration remaining(Instant until) {
    Duration left = Duration.between(Instant.now(), until);
    if (left.compareTo(Duration.ofSeconds(1)) < 0) {
      return Duration.ofSeconds(1);
    }
    return left.compareTo(HTTP_TIMEOUT) > 0 ? HTTP_TIMEOUT : left;
  }

  private JsonNode request(String uri, String token, String jsonBody, Instant until)
      throws IOException, InterruptedException {
    if (Instant.now().isAfter(until)) {
      throw new IOException("out of time before " + URI.create(uri).getPath());
    }
    HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(uri)).timeout(remaining(until))
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
    HttpRequest built = request.build();
    HttpResponse<String> response = http.send(built, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() / 100 != 2) {
      String text = response.body() == null ? "" : response.body();
      throw new HttpStatusException(built.method() + " " + URI.create(uri).getPath() + " answered HTTP "
          + response.statusCode() + ": " + text.substring(0, Math.min(400, text.length())), response.statusCode());
    }
    return mapper.readTree(response.body());
  }

  private String download(URI uri, Instant until) throws IOException, InterruptedException {
    // Bound only so the host is reachable: the URL is already signed, so this
    // request carries no credential of ours.
    HttpResponse<String> response = http.send(
        HttpRequest.newBuilder(uri).timeout(remaining(until)).GET().build(), HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() / 100 != 2) {
      throw new HttpStatusException("downloading the artifact answered HTTP " + response.statusCode(),
          response.statusCode());
    }
    return response.body();
  }

  private static void requireReachable(String downloadEndpoint, URI uri) {
    if (downloadEndpoint == null || downloadEndpoint.isBlank()) {
      return;  // nothing to check; an unbound host fails the GET, and the row says so
    }
    URI bound = URI.create(downloadEndpoint);
    if (bound.getHost() == null || !bound.getHost().equalsIgnoreCase(uri.getHost())) {
      throw new IllegalStateException("the artifact is on " + uri.getHost() + ", but the artifact connection"
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

  /** The service's spelling of a format: md, json, html, text, doctags. */
  static String toFormat(String name) {
    String wanted = name == null ? "md" : name.trim().toLowerCase(Locale.ROOT);
    return switch (wanted) {
      case "", "md", "markdown" -> "md";
      case "json" -> "json";
      case "html" -> "html";
      case "text", "txt" -> "text";
      case "doctags" -> "doctags";
      default -> throw new IllegalArgumentException("unknown output format '" + name + "'");
    };
  }

  /** What the envelope calls it — "markdown" for md, as the service's artifact_type does. */
  static String formatName(String toFormat) {
    return "md".equals(toFormat) ? "markdown" : toFormat;
  }

  private long elapsedMs(Instant until) {
    return Duration.between(until.minus(deadline), Instant.now()).toMillis();
  }

  String success(String url, String filename, String sourceUri, String format, String content,
      double processingTime, String confidence, Instant until, int attempts) {
    ObjectNode out = mapper.createObjectNode();
    out.put("status", "success");
    out.put("url", url);
    out.put("filename", filename == null ? "" : filename);
    out.put("source_uri", sourceUri == null || sourceUri.isBlank() ? url : sourceUri);
    out.put("format", format);
    out.put("content", content == null ? "" : content);
    out.put("processing_time", processingTime);
    out.put("elapsed_ms", elapsedMs(until));
    out.put("attempts", attempts);
    if (confidence != null) {
      out.put("confidence", confidence);
    }
    return write(out);
  }

  String failure(String url, String message, Instant until, int attempts) {
    init();
    ObjectNode out = mapper.createObjectNode();
    out.put("status", "failure");
    out.put("url", url == null ? "" : url);
    out.put("error", message == null ? "unknown" : message);
    out.put("elapsed_ms", until == null ? 0 : elapsedMs(until));
    out.put("attempts", attempts);
    return write(out);
  }

  private String write(ObjectNode record) {
    try {
      return mapper.writeValueAsString(record);
    } catch (IOException exception) {
      // Everything above is a string or a number; a hand-built object keeps
      // this total anyway, because NULL here would be a lost row.
      return "{\"status\":\"failure\",\"error\":\"could not serialize the result\"}";
    }
  }
}
