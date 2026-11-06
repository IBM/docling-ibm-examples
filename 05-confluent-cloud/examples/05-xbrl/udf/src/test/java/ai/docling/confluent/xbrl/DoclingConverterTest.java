// Copyright IBM Corp. 2026
// SPDX-License-Identifier: Apache-2.0
package ai.docling.confluent.xbrl;

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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/** Against a fake Docling for IBM watsonx on localhost: async task, poll, result with a presigned artifact. */
class DoclingConverterTest {

  static final String URL = "https://www.sec.gov/Archives/edgar/data/1/000000000026000001/x.htm";
  static final String DOCUMENT = "{\"texts\":[],\"tables\":[]}";

  final ObjectMapper mapper = new ObjectMapper();
  HttpServer server;
  String base;
  final List<String> submitted = new ArrayList<>();
  int submitStatus = 200;

  @BeforeEach
  void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    base = "http://127.0.0.1:" + server.getAddress().getPort();
    server.createContext("/v1/convert/source/async", exchange -> {
      submitted.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
      if (submitStatus != 200) {
        send(exchange, submitStatus, "{\"detail\":\"rejected\"}");
        return;
      }
      send(exchange, 200, "{\"task_id\":\"t1\",\"task_status\":\"pending\"}");
    });
    server.createContext("/v1/status/poll/", exchange -> send(exchange, 200, "{\"task_status\":\"success\"}"));
    server.createContext("/v1/result/", exchange -> send(exchange, 200, "{\"documents\":[{\"status\":\"success\","
        + "\"artifacts\":[{\"artifact_type\":\"json\",\"uri\":\"" + base + "/artifacts/t1.json?X-Amz-Signature=s\"}]}]}"));
    server.createContext("/artifacts/", exchange -> send(exchange, 200, DOCUMENT));
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

  DoclingConverter converter() {
    DoclingConverter converter = new DoclingConverter();
    converter.init();
    converter.pollInterval = Duration.ofMillis(10);
    return converter;
  }

  JsonNode run(String userAgent, String downloadEndpoint) throws IOException {
    return mapper.readTree(converter().convert(base, "key", downloadEndpoint, URL, userAgent,
        Instant.now().plusSeconds(30)));
  }

  @Test
  void convertsToJsonAndAsksTheServiceToFetchWithTheUserAgent() throws IOException {
    JsonNode out = run("Name name@example.com", base);
    assertEquals("success", out.path("status").asText());
    assertEquals(DOCUMENT, out.path("content").asText());

    JsonNode request = mapper.readTree(submitted.get(0));
    assertEquals(URL, request.path("sources").get(0).path("url").asText());
    assertEquals("Name name@example.com", request.path("sources").get(0).path("headers").path("User-Agent").asText());
    assertEquals("json", request.path("options").path("to_formats").get(0).asText());
  }

  @Test
  void noUserAgentSendsNoHeaders() throws IOException {
    run(null, base);
    assertFalse(mapper.readTree(submitted.get(0)).path("sources").get(0).has("headers"));
  }

  @Test
  void aRejectedRequestIsNotRetried() throws IOException {
    submitStatus = 422;
    JsonNode out = run(null, base);
    assertEquals(1, submitted.size(), "a 4xx is the request's fault; asking again only bills again");
    assertEquals("failure", out.path("status").asText());
    assertTrue(out.path("error").asText().contains("HTTP 422"));
  }

  @Test
  void theArtifactHostMustBeTheBoundOne() throws IOException {
    JsonNode out = run(null, "https://s3.us-east-1.amazonaws.com");
    assertEquals("failure", out.path("status").asText());
    assertTrue(out.path("error").asText().contains("may only reach hosts"));
  }
}
