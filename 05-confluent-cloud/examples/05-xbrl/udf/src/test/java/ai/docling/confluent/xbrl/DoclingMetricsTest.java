// Copyright IBM Corp. 2026
// SPDX-License-Identifier: Apache-2.0
package ai.docling.confluent.xbrl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Against a small DoclingDocument shaped like what Docling for IBM watsonx
 * returns for an inline-XBRL 10-K read as HTML (2026-11-04): the cover page as
 * texts, the statements as tables whose cells repeat across the columns they
 * span.
 */
class DoclingMetricsTest {

  final ObjectMapper mapper = new ObjectMapper();
  final DoclingMetrics udf = new DoclingMetrics();

  static String cell(String text, int col) {
    return "{\"text\":\"" + text + "\",\"start_col_offset_idx\":" + col + "}";
  }

  /** A row of cells; a text that repeats in the next column stands for a spanning cell. */
  static String row(String... cells) {
    StringBuilder out = new StringBuilder("[");
    int col = 0;
    for (int i = 0; i < cells.length; i++) {
      boolean spans = i > 0 && !cells[i].isEmpty() && cells[i].equals(cells[i - 1]);
      col = spans ? col : i;
      out.append(i == 0 ? "" : ",").append(cell(cells[i], col));
    }
    return out.append("]").toString();
  }

  static final String DOCUMENT = "{"
      + "\"body\":{\"children\":[{\"$ref\":\"#/texts/0\"},{\"$ref\":\"#/texts/2\"},{\"$ref\":\"#/tables/0\"}]},"
      + "\"texts\":["
      + "{\"self_ref\":\"#/texts/0\",\"text\":\"For the fiscal year ended August 31, 2026\"},"
      + "{\"self_ref\":\"#/texts/1\",\"text\":\"Not in the body\"},"
      + "{\"self_ref\":\"#/texts/2\",\"text\":\"(in thousands, except per share data)\"}],"
      + "\"tables\":[{\"self_ref\":\"#/tables/0\",\"data\":{\"grid\":["
      + row("", "2026", "2026", "2025") + ","
      + row("Total Assets", "$", "117,132", "", "$", "33,155") + ","
      + row("Net Loss", "", "(36,431", ")", "", "(214", ")") + ","
      + row("Total revenue", "49,731", "49,731", "-") + ","
      + row("Net cash used in operating activities", "$", "(43,244)", "(23,559)")
      + "]}}]}";

  JsonNode only(List<String> records, String metric) throws IOException {
    for (String record : records) {
      JsonNode node = mapper.readTree(record);
      if (metric.equals(node.path("metric").asText())) {
        return node;
      }
    }
    throw new AssertionError("no " + metric + " in " + records);
  }

  @Test
  void readsTheHeadlineFiguresOfAStatement() throws IOException {
    List<String> records = udf.metrics(mapper.readTree(DOCUMENT));
    JsonNode assets = only(records, "total_assets");
    assertEquals(117132.0, assets.path("value").asDouble());
    assertEquals(33155.0, assets.path("prior_value").asDouble());
    assertEquals("Total Assets", assets.path("label").asText());
    assertEquals("#/tables/0", assets.path("table").asText());
    assertEquals(1000, assets.path("scale").asInt(), "the text above the table says thousands");
    assertEquals("2026-08-31", assets.path("period_end").asText());
    assertEquals(4, records.size());
  }

  /** A 10-Q balance sheet: the head names the periods, over two rows, the first spanning. */
  static final String BALANCE_SHEET = "{\"texts\":[],\"tables\":[{\"self_ref\":\"#/tables/3\",\"data\":{\"grid\":["
      + row("", "September 30, 2026", "September 30, 2026", "September 30, 2026", "December 31, 2025",
          "December 31, 2025") + ","
      + row("(in thousands)", "", "", "", "", "") + ","
      + row("Total assets", "$", "117,132", "", "$", "33,155")
      + "]}}]}";

  @Test
  void theHeadNamesTheColumnsAFigureWasReadFrom() throws IOException {
    JsonNode assets = only(udf.metrics(mapper.readTree(BALANCE_SHEET)), "total_assets");
    assertEquals("September 30, 2026", assets.path("value_column").asText());
    assertEquals("December 31, 2025", assets.path("prior_column").asText());
    assertEquals(33155.0, assets.path("prior_value").asDouble());
  }

  /** StoneRidge's 10-Q: "Three months ended September 30," over "(in thousands…) | 2026 | 2025". */
  @Test
  void aHeadWhoseYearsAreOnTheRowOfTheScaleIsRead() throws IOException {
    String grid = "{\"texts\":[],\"tables\":[{\"self_ref\":\"#/tables/8\",\"data\":{\"grid\":["
        + row("", "", "", "Three months ended\\nSeptember 30,", "Three months ended\\nSeptember 30,",
            "Three months ended\\nSeptember 30,") + ","
        + row("(in thousands, except per share data)", "", "", "2026", "2026", "2025") + ","
        + row("", "", "", "", "", "") + ","
        + row("Net sales", "", "", "$", "155,802", "138,638")
        + "]}}]}";
    JsonNode revenue = only(udf.metrics(mapper.readTree(grid)), "revenue");
    assertEquals("Three months ended September 30, 2026", revenue.path("value_column").asText());
    assertEquals("Three months ended September 30, 2025", revenue.path("prior_column").asText());
  }

  @Test
  void aHeadOverTwoRowsIsJoined() throws IOException {
    String grid = "{\"texts\":[],\"tables\":[{\"self_ref\":\"#/tables/4\",\"data\":{\"grid\":["
        + row("", "Three Months Ended", "Three Months Ended", "Three Months Ended") + ","
        + row("", "Sep 30, 2026", "Sep 30, 2026", "Sep 30, 2025") + ","
        + row("Total revenue", "$", "49,731", "40,000")
        + "]}}]}";
    JsonNode revenue = only(udf.metrics(mapper.readTree(grid)), "revenue");
    assertEquals("Three Months Ended Sep 30, 2026", revenue.path("value_column").asText());
    assertEquals("Three Months Ended Sep 30, 2025", revenue.path("prior_column").asText());
  }

  @Test
  void aBracketIsNegativeAndASplitBracketToo() throws IOException {
    List<String> records = udf.metrics(mapper.readTree(DOCUMENT));
    assertEquals(-36431.0, only(records, "net_income").path("value").asDouble());
    assertEquals(-214.0, only(records, "net_income").path("prior_value").asDouble());
    assertEquals(-43244.0, only(records, "operating_cash_flow").path("value").asDouble());
  }

  @Test
  void aDashIsZeroAndASpanningCellIsReadOnce() throws IOException {
    JsonNode revenue = only(udf.metrics(mapper.readTree(DOCUMENT)), "revenue");
    assertEquals(49731.0, revenue.path("value").asDouble());
    assertEquals(0.0, revenue.path("prior_value").asDouble());
  }

  @Test
  void aStatementOfEquityIsNotAnIncomeStatement() throws IOException {
    String equity = "{\"self_ref\":\"#/tables/9\",\"data\":{\"grid\":["
        + row("Net loss", "-", "-", "-", "(19,913", ")", "(19,913", ")") + "]}}";
    String before = DOCUMENT.replace("\"tables\":[", "\"tables\":[" + equity + ",");
    JsonNode net = only(udf.metrics(mapper.readTree(before)), "net_income");
    assertEquals(-36431.0, net.path("value").asDouble(), "the income statement wins, though it comes second");
    assertEquals("#/tables/0", net.path("table").asText());

    String alone = "{\"texts\":[],\"body\":{},\"tables\":[" + equity + "]}";
    assertTrue(udf.metrics(mapper.readTree(alone)).isEmpty(), "no figure is better than a wrong one");
  }

  @Test
  void aRowOfDashesIsARealZero() throws IOException {
    String doc = "{\"texts\":[],\"body\":{},\"tables\":[{\"self_ref\":\"#/tables/1\",\"data\":{\"grid\":["
        + row("Revenue", "$", "-", "$", "-", "$", "-") + "]}}]}";
    JsonNode revenue = only(udf.metrics(mapper.readTree(doc)), "revenue");
    assertEquals(0.0, revenue.path("value").asDouble());
  }

  @Test
  void netIncomeAvailableToCommonShareholdersIsNetIncome() throws IOException {
    String doc = "{\"texts\":[],\"body\":{},\"tables\":[{\"self_ref\":\"#/tables/8\",\"data\":{\"grid\":["
        + row("Net income available to common shareholders", "$", "34,609", "$", "35,188") + "]}}]}";
    assertEquals(34609.0, only(udf.metrics(mapper.readTree(doc)), "net_income").path("value").asDouble());
  }

  @Test
  void theFirstMatchingTableWins() throws IOException {
    String second = ",{\"self_ref\":\"#/tables/1\",\"data\":{\"grid\":[" + row("Total assets", "1", "2") + "]}}";
    String document = DOCUMENT.replace("]]}}]}", "]]}}" + second + "]}");
    JsonNode assets = only(udf.metrics(mapper.readTree(document)), "total_assets");
    assertEquals("#/tables/0", assets.path("table").asText());
  }

  @Test
  void aStatementWithoutAScaleIsOne() throws IOException {
    String document = DOCUMENT.replace("(in thousands, except per share data)", "Statements");
    assertEquals(1, only(udf.metrics(mapper.readTree(document)), "total_assets").path("scale").asInt());
  }

  @Test
  void theScaleInTheTableHeadBeatsTheDocument() throws IOException {
    String document = DOCUMENT
        .replace("[{\"text\":\"\",\"start_col_offset_idx\":0}",
            "[{\"text\":\"(in millions)\",\"start_col_offset_idx\":0}");
    assertEquals(1_000_000, only(udf.metrics(mapper.readTree(document)), "total_assets").path("scale").asInt());
  }

  @Test
  void theCoverPeriodIsAnIsoDate() throws IOException {
    JsonNode texts = mapper.readTree(
        "[{\"text\":\"FORM 10-Q\"},{\"text\":\"FOR THE QUARTERLY PERIOD ENDED SEPTEMBER 30, 2026\"}]");
    assertEquals("2026-09-30", DoclingMetrics.periodEnd(texts));
    assertEquals("2025-12-31", DoclingMetrics.periodEnd(mapper.readTree(
        "[{\"text\":\"For the fiscal year ended\"},{\"text\":\"December 31, 2025\"}]")));
    assertEquals("2026-08-31", DoclingMetrics.periodEnd(mapper.readTree(
        "[{\"text\":\"FORM 10-K\"},{\"text\":\"August 31 , 2026\"}]")));
    assertNull(DoclingMetrics.periodEnd(mapper.readTree("[{\"text\":\"nothing here\"}]")));
  }

  static String envelope(String content) {
    return new ObjectMapper().createObjectNode().put("status", "success").put("content", content).toString();
  }

  @Test
  void aDocumentOverJacksonsDefaultStringCapIsRead() throws IOException {
    // A big 10-Q: the DoclingDocument inside the envelope is a string of over 20 000 000 characters.
    String padding = "x".repeat(21_000_000);
    String big = DOCUMENT.replace("\"texts\":[", "\"texts\":[{\"self_ref\":\"#/texts/9\",\"text\":\"" + padding + "\"},");
    assertTrue(big.length() > 20_000_000);
    String[] records = udf.fromEnvelope(envelope(big));
    assertEquals(4, records.length);
    assertEquals("total_assets", mapper.readTree(records[0]).path("metric").asText());
  }

  @Test
  void theEnvelopeIsReadWithoutWhatItNeverLooksAt() throws IOException {
    String heavy = DOCUMENT.replace("\"tables\":[{\"self_ref\":\"#/tables/0\",",
        "\"pages\":{\"1\":{\"size\":{}}},\"groups\":[{\"self_ref\":\"#/groups/0\"}],"
            + "\"tables\":[{\"self_ref\":\"#/tables/0\",\"prov\":[{\"page_no\":1}],"
            + "\"data\":{\"table_cells\":[{\"text\":\"Total Assets\"}]},");
    JsonNode read = udf.readDocument(heavy);
    assertTrue(read.path("pages").isMissingNode());
    assertTrue(read.path("groups").isMissingNode());
    assertTrue(read.path("tables").get(0).path("prov").isMissingNode());
    assertTrue(read.path("tables").get(0).path("data").path("table_cells").isMissingNode());
    assertEquals("Total Assets", read.path("tables").get(0).path("data").path("grid").get(1).get(0).path("text").asText());
    assertEquals(4, udf.fromEnvelope(envelope(heavy)).length);
  }

  @Test
  void aSuccessfulConversionGivesItsFigures() throws IOException {
    String[] records = udf.fromEnvelope(envelope(DOCUMENT));
    assertEquals(4, records.length);
    assertEquals("total_assets", mapper.readTree(records[0]).path("metric").asText());
  }

  @Test
  void aFailureIsARecordNotAnAbsence() throws IOException {
    JsonNode failed = mapper.readTree(udf.fromEnvelope("{\"status\":\"failure\",\"error\":\"HTTP 403\"}")[0]);
    assertEquals(DoclingMetrics.FAILED, failed.path("metric").asText());
    assertEquals("HTTP 403", failed.path("label").asText());
    assertTrue(failed.path("value").isNull());

    assertEquals(DoclingMetrics.FAILED, mapper.readTree(udf.fromEnvelope("not json")[0]).path("metric").asText());
    assertEquals(DoclingMetrics.FAILED, mapper.readTree(udf.fromEnvelope(null)[0]).path("metric").asText());
    assertEquals(DoclingMetrics.FAILED, mapper.readTree(udf.fromEnvelope("")[0]).path("metric").asText());
    assertEquals(DoclingMetrics.FAILED,
        mapper.readTree(udf.fromEnvelope(envelope("# markdown, not a DoclingDocument"))[0]).path("metric").asText());
  }

  @Test
  void aDocumentWithNoStatementsIsARecordToo() throws IOException {
    assertTrue(udf.metrics(mapper.readTree("{\"texts\":[],\"tables\":[],\"body\":{}}")).isEmpty());
    String[] records = udf.fromEnvelope(envelope("{\"texts\":[],\"tables\":[],\"body\":{}}"));
    assertEquals(1, records.length);
    assertEquals(DoclingMetrics.NOTHING_FOUND, mapper.readTree(records[0]).path("metric").asText());
  }

  @Test
  void evalAnswersEvenWhenTheFunctionIsNotBound() throws Exception {
    udf.init();
    CompletableFuture<String[]> result = new CompletableFuture<>();
    udf.eval(result, "missing_conn", null, "https://www.sec.gov/x.htm", "Name name@example.com");
    JsonNode record = mapper.readTree(result.get(10, TimeUnit.SECONDS)[0]);
    assertEquals(DoclingMetrics.FAILED, record.path("metric").asText());
    assertTrue(record.path("label").asText().contains("has no endpoint"));
    udf.close();
  }
}
