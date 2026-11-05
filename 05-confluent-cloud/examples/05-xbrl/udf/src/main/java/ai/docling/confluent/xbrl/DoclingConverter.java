package ai.docling.confluent.xbrl;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

import org.apache.flink.table.functions.FunctionContext;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * One Docling conversion, to a DoclingDocument (JSON), from inside {@link DoclingMetrics}.
 *
 * <p>The same calls as {@code dc_convert} in {@code common/udf-java} (submit, poll, fetch the result, download
 * the presigned artifact), with the two differences this example needs:
 * <ul>
 *   <li>the {@code User-Agent} the <em>service</em> fetches the URL with, because the SEC answers 403 without
 *       one;</li>
 *   <li>no cap on the size of the document: it never leaves {@link DoclingMetrics}, so a row's 4 MB is no
 *       limit.</li>
 * </ul>
 *
 * <p>The connections work as there: Confluent Cloud injects a bound {@code CONNECTION}'s endpoint and token as
 * job parameters named {@code <name>.endpoint} and {@code <name>.token}.
 *
 * <p>{@link #call} never throws: what it returns is a JSON envelope, {@code {"status":"success","content":...}}
 * or {@code {"status":"failure","error":...}}.
 */
class DoclingConverter {

  /** Total time one record may take, from eval() — queueing and all attempts — under the ~60 s limit. */
  static final Duration DEADLINE = Duration.ofSeconds(45);
  static final int MAX_ATTEMPTS = 3;
  static final Duration POLL_INTERVAL = Duration.ofSeconds(1);
  static final Duration HTTP_TIMEOUT = Duration.ofSeconds(30);

  /** Test seam: the poll interval, shortened by the tests. */
  Duration pollInterval = POLL_INTERVAL;
  /** Test seam: the deadline, shortened by the tests. */
  Duration deadline = DEADLINE;

  private ObjectMapper mapper;
  private HttpClient http;
  private FunctionContext context;

  void open(FunctionContext context) {
    this.context = context;
    init();
  }

  void init() {
    if (mapper == null) {
      // The envelope holds the whole DoclingDocument as one string: a large 10-Q is 20 MB, Jackson's default cap.
      mapper = new ObjectMapper(JsonFactory.builder()
          .streamReadConstraints(StreamReadConstraints.builder().maxStringLength(Integer.MAX_VALUE).build())
          .build());
    }
    if (http == null) {
      http = HttpClient.newBuilder()
          .connectTimeout(Duration.ofSeconds(10))
          .followRedirects(HttpClient.Redirect.NORMAL)
          .build();
    }
  }

  /** One conversion, on the calling thread, from connection names. Never throws. */
  String call(String doclingConn, String downloadConn, String url, String userAgent, Instant until) {
    init();
    String endpoint = jobParameter(doclingConn, "endpoint");
    if (endpoint == null || endpoint.isBlank()) {
      return failure(url, "connection '" + doclingConn + "' has no endpoint. Is the function bound "
          + "to it with USING CONNECTIONS, and is that the connection's name?", until, 0);
    }
    String downloadEndpoint = downloadConn == null || downloadConn.isBlank()
        ? null : jobParameter(downloadConn, "endpoint");
    return convert(endpoint, jobParameter(doclingConn, "token"), downloadEndpoint, url, userAgent, until);
  }

  /** The whole call, from resolved endpoints — what the tests drive. Never throws. */
  String convert(String endpoint, String token, String downloadEndpoint, String url, String userAgent,
      Instant until) {
    init();
    if (url == null || url.isBlank()) {
      return failure(url, "no url", until, 0);
    }
    String last = null;
    int attempt = 1;
    for (; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        Attempt result = once(endpoint, token, downloadEndpoint, url, userAgent, until, attempt);
        last = result.envelope;
        if (!result.retryable) {
          return last;
        }
      } catch (Exception exception) {  // NOPMD — a bad row must not stop the stream
        last = failure(url, exception.getClass().getSimpleName() + ": " + exception.getMessage(), until, attempt);
        // A 4xx is the request's fault — a URL the service will not fetch — and asking again only bills again.
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

  private Attempt once(String endpoint, String token, String downloadEndpoint, String url, String userAgent,
      Instant until, int attempt) throws IOException, InterruptedException {
    String base = endpoint.replaceAll("/+$", "");

    ObjectNode body = mapper.createObjectNode();
    ObjectNode source = body.putArray("sources").addObject().put("kind", "http").put("url", url);
    if (userAgent != null && !userAgent.isBlank()) {
      source.putObject("headers").put("User-Agent", userAgent);
    }
    body.putObject("options").putArray("to_formats").add("json");

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

    // In the body — a self-hosted docling-serve: {"document": {"json_content": ...}}.
    JsonNode inBody = response.path("document");
    if (inBody.isObject()) {
      JsonNode content = inBody.path("json_content");
      return new Attempt(success(url, content.isTextual() ? content.asText() : content.toString(), until, attempt),
          false);
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
    URI uri = URI.create(artifacts.get(0).path("uri").asText());
    requireReachable(downloadEndpoint, uri);
    return new Attempt(success(url, download(uri, until), until, attempt), false);
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

  private String success(String url, String content, Instant until, int attempts) {
    ObjectNode out = mapper.createObjectNode();
    out.put("status", "success");
    out.put("url", url);
    out.put("content", content == null ? "" : content);
    out.put("elapsed_ms", elapsedMs(until));
    out.put("attempts", attempts);
    return write(out);
  }

  private String failure(String url, String message, Instant until, int attempts) {
    ObjectNode out = mapper.createObjectNode();
    out.put("status", "failure");
    out.put("url", url == null ? "" : url);
    out.put("error", message == null ? "unknown" : message);
    out.put("elapsed_ms", until == null ? 0 : elapsedMs(until));
    out.put("attempts", attempts);
    return write(out);
  }

  private long elapsedMs(Instant until) {
    return Duration.between(until.minus(deadline), Instant.now()).toMillis();
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
