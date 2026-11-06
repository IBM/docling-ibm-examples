package ai.docling.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
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
 * watsonx did on 2026-10-06: async task, poll, result with a presigned
 * markdown artifact — and, switched by {@link #inBody}, like a self-hosted
 * docling-serve that answers in the body.
 */
class DoclingConvertTest {

  static final String URL = "https://arxiv.org/pdf/2408.09869";
  static final String MARKDOWN = "# Docling Technical Report\n\nSome text.";

  final ObjectMapper mapper = new ObjectMapper();
  HttpServer server;
  String base;
  final List<String> submitted = new ArrayList<>();
  final List<String> apiKeys = new ArrayList<>();
  final AtomicInteger tasks = new AtomicInteger();
  /** Fail the first n tasks, with retryable as given. */
  int failFirst;
  boolean failRetryable = true;
  int submitStatus = 200;
  boolean inBody;

  @BeforeEach
  void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    base = "http://127.0.0.1:" + server.getAddress().getPort();
    server.createContext("/v1/convert/source/async", exchange -> {
      submitted.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
      apiKeys.add(exchange.getRequestHeaders().getFirst("X-Api-Key"));
      int n = tasks.incrementAndGet();
      if (submitStatus != 200) {
        send(exchange, submitStatus, "{\"detail\":\"rejected\"}");
        return;
      }
      send(exchange, 200, "{\"task_id\":\"t" + n + "\",\"task_status\":\"pending\"}");
    });
    server.createContext("/v1/status/poll/", exchange -> {
      int n = Integer.parseInt(exchange.getRequestURI().getPath().replaceAll(".*/t", ""));
      if (n <= failFirst) {
        send(exchange, 200, "{\"task_status\":\"failure\",\"error_message\":\"Internal processing error.\","
            + "\"failure\":{\"message\":\"Internal processing error.\",\"retryable\":" + failRetryable + "}}");
        return;
      }
      send(exchange, 200, "{\"task_status\":\"success\"}");
    });
    server.createContext("/v1/result/", exchange -> {
      if (inBody) {
        send(exchange, 200, "{\"processing_time\":1.5,\"status\":\"success\",\"document\":{\"filename\":"
            + "\"2408.09869.pdf\",\"md_content\":" + mapper.writeValueAsString(MARKDOWN) + "}}");
        return;
      }
      String id = exchange.getRequestURI().getPath().replaceAll(".*/", "");
      send(exchange, 200, "{\"processing_time\":2.1,\"documents\":[{\"source_index\":0,\"source_uri\":\"" + URL
          + "\",\"filename\":\"2408.09869\",\"status\":\"success\",\"errors\":[],\"artifacts\":[{"
          + "\"artifact_type\":\"markdown\",\"mime_type\":\"text/markdown\",\"uri\":\"" + base + "/artifacts/"
          + id + ".md?X-Amz-Signature=s\"}],\"confidence\":{\"mean_grade\":\"good\"}}]}");
    });
    server.createContext("/artifacts/", exchange -> send(exchange, 200, MARKDOWN));
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

  DoclingConvert udf() {
    DoclingConvert udf = new DoclingConvert();
    udf.init();
    udf.pollInterval = Duration.ofMillis(10);
    return udf;
  }

  JsonNode run(String downloadEndpoint) throws IOException {
    return mapper.readTree(udf().convert(base, "key-1", downloadEndpoint, URL, "md"));
  }

  @Test
  void fetchesThePresignedArtifact() throws IOException {
    JsonNode out = run(base);
    assertEquals("success", out.path("status").asText());
    assertEquals(MARKDOWN, out.path("content").asText());
    assertEquals("markdown", out.path("format").asText());
    assertEquals("2408.09869", out.path("filename").asText());
    assertEquals(URL, out.path("url").asText());
    assertEquals("good", out.path("confidence").asText());
    assertEquals(2.1, out.path("processing_time").asDouble());
    assertEquals(1, out.path("attempts").asInt());
    assertTrue(out.path("elapsed_ms").asLong() >= 0);

    JsonNode request = mapper.readTree(submitted.get(0));
    assertEquals("http", request.path("sources").get(0).path("kind").asText());
    assertEquals(URL, request.path("sources").get(0).path("url").asText());
    assertEquals("md", request.path("options").path("to_formats").get(0).asText());
    assertFalse(request.has("target"), "no target: each service answers in its default shape");
    assertEquals("key-1", apiKeys.get(0));
  }

  @Test
  void readsAnAnswerInTheBody() throws IOException {
    inBody = true;
    JsonNode out = run(null);
    assertEquals("success", out.path("status").asText());
    assertEquals(MARKDOWN, out.path("content").asText());
    assertEquals("2408.09869.pdf", out.path("filename").asText());
  }

  @Test
  void aRetryableTaskFailureIsAskedAgain() throws IOException {
    failFirst = 1;
    JsonNode out = run(base);
    assertEquals(2, tasks.get());
    assertEquals("success", out.path("status").asText());
    assertEquals(2, out.path("attempts").asInt());
  }

  @Test
  void aRetryableFailureEveryTimeIsReturnedAsAValue() throws IOException {
    failFirst = 99;
    JsonNode out = run(base);
    assertEquals(DoclingConvert.MAX_ATTEMPTS, tasks.get());
    assertEquals("failure", out.path("status").asText());
    assertTrue(out.path("error").asText().contains("Internal processing error"));
  }

  @Test
  void aNonRetryableFailureIsNotRetried() throws IOException {
    failFirst = 99;
    failRetryable = false;
    JsonNode out = run(base);
    assertEquals(1, tasks.get());
    assertEquals("failure", out.path("status").asText());
  }

  @Test
  void aRejectedRequestIsNotRetried() throws IOException {
    submitStatus = 422;
    JsonNode out = run(base);
    assertEquals(1, tasks.get(), "a 4xx is the request's fault; asking again only bills again");
    assertEquals("failure", out.path("status").asText());
    assertTrue(out.path("error").asText().contains("HTTP 422"));
  }

  @Test
  void aServerErrorIsRetried() throws IOException {
    submitStatus = 503;
    run(base);
    assertEquals(DoclingConvert.MAX_ATTEMPTS, tasks.get());
  }

  @Test
  void theArtifactHostMustBeTheBoundOne() throws IOException {
    JsonNode out = run("https://s3.us-east-1.amazonaws.com");
    assertEquals("failure", out.path("status").asText());
    assertTrue(out.path("error").asText().contains("may only reach hosts"));
  }

  @Test
  void aTaskThatOutlivesTheDeadlineIsAFailureValueInTime() throws IOException {
    server.removeContext("/v1/status/poll/");
    server.createContext("/v1/status/poll/", exchange -> send(exchange, 200, "{\"task_status\":\"started\"}"));
    long start = System.nanoTime();
    String out = udf().convert(base, null, null, URL, "md", Instant.now().plusMillis(300));
    long millis = (System.nanoTime() - start) / 1_000_000;
    assertTrue(millis < 2_000, "answered after " + millis + " ms");
    JsonNode node = mapper.readTree(out);
    assertEquals("failure", node.path("status").asText());
    String message = node.path("error").asText();
    assertTrue(message.contains("did not finish") || message.contains("out of time"), message);
  }

  @Test
  void badInputsAreFailureValues() throws IOException {
    DoclingConvert udf = udf();
    assertEquals("failure", mapper.readTree(udf.convert(base, null, null, "", "md")).path("status").asText());
    JsonNode format = mapper.readTree(udf.convert(base, null, null, URL, "pdf"));
    assertEquals("failure", format.path("status").asText());
    assertTrue(format.path("error").asText().contains("unknown output format"));
    assertEquals(0, tasks.get());
  }

  @Test
  void anUnboundConnectionIsAFailureValue() throws Exception {
    CompletableFuture<String> result = new CompletableFuture<>();
    udf().eval(result, "missing_conn", null, URL);
    JsonNode out = mapper.readTree(result.get(10, TimeUnit.SECONDS));
    assertEquals("failure", out.path("status").asText());
    assertTrue(out.path("error").asText().contains("has no endpoint"));
  }

  @Test
  void evalCompletesTheFutureOffTheCallingThread() throws Exception {
    DoclingConvert udf = udf();
    CompletableFuture<String> a = new CompletableFuture<>();
    udf.eval(a, null, null, "", "md");
    assertEquals("failure", mapper.readTree(a.get(10, TimeUnit.SECONDS)).path("status").asText());
    udf.close();
    CompletableFuture<String> late = new CompletableFuture<>();
    udf.eval(late, null, null, URL, "md");  // after close: a new pool, still an answer
    assertTrue(late.get(10, TimeUnit.SECONDS).contains("\"status\":\"failure\""));
  }
}
