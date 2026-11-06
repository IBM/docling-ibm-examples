package ai.docling.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * Against a fake docling-serve on localhost that answers like Docling for IBM
 * watsonx did on 2026-10-05: async task, poll, result with a presigned
 * artifact, and the artifact an ExtractionDocumentResult.
 */
class DoclingExtractTest {

  static final String TARGET = "{\"output_schema\":{\"type\":\"object\",\"properties\":"
      + "{\"total_amount\":{\"type\":\"number\"}}}}";
  static final String URL = "https://cdn.example/xet/abc123?Signature=x";

  static final String OK_PAGE = "{\"scope\":{\"kind\":\"page\",\"page_no\":1},"
      + "\"extracted_data\":{\"total_amount\":545.32},\"raw_text\":\"{}\",\"errors\":[],"
      + "\"validation_status\":\"passed\",\"inference_metadata\":{\"generation_time\":-1.0}}";
  static final String TIMED_OUT_PAGE = "{\"scope\":{\"kind\":\"page\",\"page_no\":1},"
      + "\"extracted_data\":{},\"raw_text\":null,\"errors\":[{\"component_type\":\"model\","
      + "\"module_name\":\"ExtractionVlmPipeline\",\"error_message\":\"Read timed out\","
      + "\"category\":\"inference_failure\",\"page_no\":1}],\"validation_status\":\"not_run\","
      + "\"inference_metadata\":null}";

  /** What Docling answers for a page whose answer broke the schema: data only in raw_text. */
  static final String INVALID_PAGE = "{\"scope\":{\"kind\":\"page\",\"page_no\":1},"
      + "\"extracted_data\":null,\"raw_text\":\"{\\\"total_amount\\\": null}\",\"errors\":[{\"component_type\":"
      + "\"model\",\"module_name\":\"ExtractionVlmPipeline\",\"error_message\":\"Schema validation at "
      + "$.total_amount: None is not of type 'number'\",\"category\":\"inference_failure\",\"page_no\":1}],"
      + "\"validation_status\":\"failed\",\"inference_metadata\":{\"generation_time\":-1.0}}";

  final ObjectMapper mapper = new ObjectMapper();
  HttpServer server;
  String base;
  final List<String> submitted = new ArrayList<>();
  final List<String> apiKeys = new ArrayList<>();
  /** What each successive task's artifact says, in order; the last repeats. */
  final List<String> artifacts = new ArrayList<>();
  final AtomicInteger tasks = new AtomicInteger();
  String taskFailure;
  int submitStatus = 200;

  @BeforeEach
  void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    base = "http://127.0.0.1:" + server.getAddress().getPort();
    server.createContext("/v1/extract/source/async", exchange -> {
      submitted.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
      apiKeys.add(exchange.getRequestHeaders().getFirst("X-Api-Key"));
      int n = tasks.incrementAndGet();
      if (submitStatus != 200) {
        send(exchange, submitStatus, "{\"detail\":\"Extraction preset 'x' is not allowed.\"}");
        return;
      }
      send(exchange, 200, "{\"task_id\":\"t" + n + "\",\"task_status\":\"pending\"}");
    });
    server.createContext("/v1/status/poll/", exchange -> {
      if (taskFailure != null) {
        send(exchange, 200, "{\"task_status\":\"failure\",\"error_message\":\"Internal processing error.\","
            + "\"failure\":{\"message\":\"Internal processing error.\",\"retryable\":" + taskFailure + "}}");
        return;
      }
      send(exchange, 200, "{\"task_status\":\"success\"}");
    });
    server.createContext("/v1/result/", exchange -> {
      String id = exchange.getRequestURI().getPath().replaceAll(".*/", "");
      send(exchange, 200, "{\"processing_time\":5.2,\"documents\":[{\"source_index\":0,\"filename\":\"abc123\","
          + "\"status\":\"success\",\"artifacts\":[{\"artifact_type\":\"json\",\"uri\":\"" + base
          + "/artifacts/" + id + ".extraction.json?X-Amz-Signature=s\"}]}]}");
    });
    server.createContext("/artifacts/", exchange -> {
      int n = Integer.parseInt(exchange.getRequestURI().getPath().replaceAll("\\D", ""));
      String page = artifacts.get(Math.min(n, artifacts.size()) - 1);
      boolean failed = page.contains("inference_failure");
      send(exchange, 200, "{\"source_index\":0,\"source_uri\":\"" + URL + "\",\"filename\":\"abc123\","
          + "\"status\":\"" + (failed ? "partial_success" : "success") + "\",\"errors\":[],\"items\":["
          + page + "]}");
    });
    server.start();
  }

  @AfterEach
  void stop() {
    server.stop(0);
  }

  static void send(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  DoclingExtract udf() {
    DoclingExtract udf = new DoclingExtract();
    udf.init();
    udf.pollInterval = Duration.ofMillis(10);
    return udf;
  }

  JsonNode run(String downloadEndpoint) throws IOException {
    return mapper.readTree(udf().extract(base, "key-1", downloadEndpoint, URL, TARGET, "presigned_url"));
  }

  @Test
  void returnsTheArtifactAsDoclingWroteIt() throws IOException {
    artifacts.add(OK_PAGE);
    JsonNode out = run(base);
    assertEquals("success", out.path("status").asText());
    assertEquals(545.32, out.path("items").get(0).path("extracted_data").path("total_amount").asDouble());
    assertEquals("passed", out.path("items").get(0).path("validation_status").asText());

    JsonNode request = mapper.readTree(submitted.get(0));
    assertEquals(mapper.readTree(TARGET), request.path("extraction_target"), "the target passes through unchanged");
    assertEquals("http", request.path("sources").get(0).path("kind").asText());
    assertEquals(URL, request.path("sources").get(0).path("url").asText());
    assertEquals("presigned_url", request.path("target").path("kind").asText());
    assertFalse(request.has("options"), "no options: the client default page_range fails the SaaS");
    assertEquals("key-1", apiKeys.get(0));
  }

  @Test
  void asksAgainWhenAPageTimedOut() throws IOException {
    artifacts.add(TIMED_OUT_PAGE);
    artifacts.add(OK_PAGE);
    JsonNode out = run(base);
    assertEquals(2, tasks.get());
    assertEquals("success", out.path("status").asText());
  }

  @Test
  void keepsTheLastAnswerWhenEveryAttemptTimedOut() throws IOException {
    artifacts.add(TIMED_OUT_PAGE);
    JsonNode out = run(base);
    assertEquals(DoclingExtract.MAX_ATTEMPTS, tasks.get());
    assertEquals("partial_success", out.path("status").asText());
    assertEquals("not_run", out.path("items").get(0).path("validation_status").asText());
  }

  @Test
  void aWrongAnswerIsNotRetried() throws IOException {
    artifacts.add(INVALID_PAGE);
    JsonNode out = run(base);
    assertEquals(1, tasks.get(), "a schema violation is quality, not availability");
    assertEquals("failed", out.path("items").get(0).path("validation_status").asText());
  }

  @Test
  void aRetryableTaskFailureIsRetriedThenReturnedAsAValue() throws IOException {
    taskFailure = "true";
    JsonNode out = run(base);
    assertEquals(DoclingExtract.MAX_ATTEMPTS, tasks.get());
    assertEquals("failure", out.path("status").asText());
    assertTrue(out.path("errors").get(0).path("error_message").asText().contains("Internal processing error"));
    assertEquals(0, out.path("items").size());
    assertEquals("abc123", out.path("filename").asText());
  }

  @Test
  void aNonRetryableFailureIsNotRetried() throws IOException {
    taskFailure = "false";
    run(base);
    assertEquals(1, tasks.get());
  }

  @Test
  void aRejectedRequestIsAFailureValue() throws IOException {
    submitStatus = 422;
    JsonNode out = run(base);
    assertEquals("failure", out.path("status").asText());
    assertTrue(out.path("errors").get(0).path("error_message").asText().contains("HTTP 422"));
  }

  @Test
  void theArtifactHostMustBeTheBoundOne() throws IOException {
    artifacts.add(OK_PAGE);
    JsonNode out = run("https://s3.us-east-1.amazonaws.com");
    assertEquals("failure", out.path("status").asText());
    assertTrue(out.path("errors").get(0).path("error_message").asText().contains("may only reach hosts"));
  }

  @Test
  void aTaskThatOutlivesTheDeadlineIsAFailureValueInTime() throws IOException {
    server.removeContext("/v1/status/poll/");
    server.createContext("/v1/status/poll/", exchange -> send(exchange, 200, "{\"task_status\":\"started\"}"));
    long start = System.nanoTime();
    String out = udf().extract(base, null, null, URL, TARGET, null, java.time.Instant.now().plusMillis(300));
    long millis = (System.nanoTime() - start) / 1_000_000;
    assertTrue(millis < 2_000, "answered after " + millis + " ms");
    assertEquals("failure", mapper.readTree(out).path("status").asText());
    String message = mapper.readTree(out).path("errors").get(0).path("error_message").asText();
    assertTrue(message.contains("did not finish") || message.contains("out of time"), message);
  }

  @Test
  void badInputsAreFailureValues() throws IOException {
    DoclingExtract udf = udf();
    assertEquals("failure", mapper.readTree(udf.extract(base, null, null, "", TARGET, null)).path("status").asText());
    assertEquals("failure", mapper.readTree(udf.extract(base, null, null, URL, "{nope", null)).path("status").asText());
    assertEquals(0, tasks.get());
  }

  @Test
  void anUnboundConnectionIsAFailureValue() throws Exception {
    CompletableFuture<String> result = new CompletableFuture<>();
    udf().eval(result, "missing_conn", null, URL, TARGET);
    JsonNode out = mapper.readTree(result.get(10, TimeUnit.SECONDS));
    assertEquals("failure", out.path("status").asText());
    assertTrue(out.path("errors").get(0).path("error_message").asText().contains("has no endpoint"));
  }

  @Test
  void evalCompletesTheFutureOffTheCallingThread() throws Exception {
    DoclingExtract udf = udf();
    CompletableFuture<String> a = new CompletableFuture<>();
    CompletableFuture<String> b = new CompletableFuture<>();
    udf.eval(a, null, null, "", TARGET);
    udf.eval(b, null, null, URL, "{nope");
    assertEquals("failure", mapper.readTree(a.get(10, TimeUnit.SECONDS)).path("status").asText());
    assertEquals("failure", mapper.readTree(b.get(10, TimeUnit.SECONDS)).path("status").asText());
    udf.close();
    CompletableFuture<String> late = new CompletableFuture<>();
    udf.eval(late, null, null, URL, TARGET);  // after close: a new pool, still an answer
    assertTrue(late.get(10, TimeUnit.SECONDS).contains("\"status\":\"failure\""));
  }
}
