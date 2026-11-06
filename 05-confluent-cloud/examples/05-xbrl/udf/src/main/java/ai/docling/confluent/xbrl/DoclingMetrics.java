package ai.docling.confluent.xbrl;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionContext;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The headline financial figures of a filing: converted by Docling, and read,
 * in one function.
 *
 * <pre>
 *   dc_metrics(docling_connection, artifact_connection, url, user_agent)
 *       -&gt;  ARRAY&lt;STRING&gt;, one JSON object per metric
 * </pre>
 *
 * <h2>Why it converts too</h2>
 *
 * The DoclingDocument of a 10-Q is 14 MB of JSON, and a Confluent Cloud row is
 * 4 MB in and out, a limit of the platform. A document that large cannot be
 * the output of one function and the input of the next. So the conversion
 * happens in here, with {@link DoclingConverter} (the calls behind
 * {@code dc_convert}, in-process and with no cap on the document), and
 * what leaves is the few hundred bytes of figures. The document is never a
 * row, and is not kept anywhere: converting again means paying again.
 *
 * <p>The reader does not build what it never looks at: {@link #DROPPED} (the
 * table cells a grid repeats, the provenance, the pages) is skipped while
 * parsing, so the tree in memory is a small part of the document.
 *
 * <p>Nothing is stored, so a failure has to be visible in the output itself:
 * a conversion that failed, or a document with no figure in it, is one record
 * whose {@code metric} is {@link #FAILED} or {@link #NOTHING_FOUND} and whose
 * {@code label} says why. Every filing leaves at least one row.
 *
 * <h2>Where the figures are</h2>
 *
 * Docling's XBRL backend writes every fact into the document's
 * {@code key_value_items}, with its concept, period and unit. The service
 * behind this repo cannot be given a taxonomy, so it converts the filing's
 * <em>inline XBRL</em> {@code .htm} as the HTML it also is: a document of
 * {@code texts} and {@code tables}, and no {@code key_value_items} at all. The
 * statements are tables, so that is where the figures are read from — by the
 * label of the row, which is the only handle a table gives:
 *
 * <pre>
 *   | Total assets | $ | 117,132 |  | $ | 33,155 |      -&gt;  total_assets  117132  33155
 *   | Net loss     |   | (36,431)|  |   | (214)  |      -&gt;  net_income   -36431  -214
 * </pre>
 *
 * The first two numbers of the row are the current and the prior period. The
 * first table with a matching label wins. This is a reading of a layout, not of
 * XBRL concepts, and it says so in what it returns: {@code label} is the row's
 * own text, {@code table} is where it came from, and {@code scale} is what the
 * statement says its figures are in (1, 1000, 1000000).
 *
 * <h2>What a record carries</h2>
 *
 * <pre>
 *   {"metric":"total_assets","label":"Total Assets","value":117132.0,"prior_value":33155.0,
 *    "value_column":"August 31, 2026","prior_column":"August 31, 2025",
 *    "scale":1,"table":"#/tables/61","period_end":"2026-08-31"}
 * </pre>
 *
 * {@code value} is as reported; multiply by {@code scale} for dollars.
 * {@code value_column} and {@code prior_column} are what the head of the table
 * says above each of the two figures, so that "prior" is a period and not a guess.
 * {@code period_end} is the report's, from its cover page, and is the same on
 * every record of a filing.
 */
public class DoclingMetrics extends AsyncScalarFunction {

  private static final long serialVersionUID = 1L;

  /**
   * Conversions in flight at once, per function instance. Almost all of a conversion is waiting on the
   * service, and the 45 s deadline runs from the moment Flink hands over the record, queueing included: with
   * two threads, a filing behind two slow ones was out of time before its first request.
   */
  static final int THREADS = 8;

  /** Filings parsed at once. A DoclingDocument is up to 20 MB of JSON, so this, not the waiting, is what costs memory. */
  static final int PARSING = 2;

  /** Never read, and most of the 14 MB: skipped while parsing, at any depth. */
  static final Set<String> DROPPED = Set.of("table_cells", "prov", "pages", "pictures", "groups");

  /** The {@code metric} of a filing that could not be converted. */
  static final String FAILED = "conversion_failed";
  /** The {@code metric} of a filing that was converted and has none of the figures. */
  static final String NOTHING_FOUND = "no_figures_found";

  /** metric -&gt; the labels of its row, lower-cased and trimmed. Order is the order of the output. */
  static final Map<String, Pattern> METRICS = new LinkedHashMap<>();

  static {
    metric("revenue", "(?:total )?(?:net )?(?:revenues?|sales)(?:, net)?|total net revenues?|net revenues?");
    metric("gross_profit", "gross (?:profit|margin)");
    metric("operating_income", "(?:income|loss|\\(loss\\) income|income \\(loss\\)) from operations"
        + "|operating (?:income|loss|income \\(loss\\)|\\(loss\\) income)");
    metric("net_income", "net (?:income available to common (?:stock|share)holders|income|loss|earnings|income \\(loss\\)|\\(loss\\) income|\\(loss\\) earnings"
        + "|earnings \\(loss\\)|loss and comprehensive loss|income and comprehensive income)");
    metric("total_assets", "total assets");
    metric("total_current_assets", "total current assets");
    metric("total_liabilities", "total liabilities");
    metric("total_current_liabilities", "total current liabilities");
    metric("stockholders_equity", "total (?:stockholders|shareholders)[’']? ?(?:equity|deficit|\\(deficit\\) equity"
        + "|equity \\(deficit\\))|total equity");
    metric("cash", "cash and cash equivalents");
    metric("operating_cash_flow", "net cash (?:provided by|used in|\\(used in\\) provided by|provided by \\(used in\\)"
        + "|provided by \\(used for\\)|used in \\(provided by\\)|from|flows? from)[^a-z]*(?:\\(?used in\\)? )?"
        + "operating activities");
  }

  private static void metric(String name, String labels) {
    METRICS.put(name, Pattern.compile("^(?:" + labels + ")$"));
  }

  private static final Pattern NUMBER = Pattern.compile("^\\(?\\s*\\$?\\s*(\\d[\\d,]*(?:\\.\\d+)?)\\s*\\)?$");
  private static final Pattern SCALE = Pattern.compile("in (thousands|millions)", Pattern.CASE_INSENSITIVE);
  private static final String DATE_TEXT = "([A-Za-z]+\\.? \\d{1,2} ?, ?\\d{4})";
  private static final Pattern PERIOD = Pattern.compile("(?:period|year|quarter) ended:?\\s*" + DATE_TEXT,
      Pattern.CASE_INSENSITIVE);
  private static final Pattern ENDED = Pattern.compile("(?:period|year|quarter) ended:?$", Pattern.CASE_INSENSITIVE);
  private static final Pattern DATE = Pattern.compile(DATE_TEXT);
  private static final Pattern DATE_ONLY = Pattern.compile("^" + DATE_TEXT + "$");
  private static final DateTimeFormatter LONG_DATE = DateTimeFormatter.ofPattern("MMMM d yyyy", Locale.ENGLISH);

  /** The cover page is in the first texts; looking further finds the periods of other statements. */
  static final int COVER_TEXTS = 120;

  private transient ObjectMapper mapper;
  private transient DoclingConverter converter;
  private transient ExecutorService pool;
  private transient Semaphore parsing;

  private ObjectMapper mapper() {
    if (mapper == null) {
      // The envelope holds the whole DoclingDocument as one string: a large 10-Q
      // is 20 MB, Jackson's default cap. The document never leaves this function.
      mapper = new ObjectMapper(JsonFactory.builder()
          .streamReadConstraints(StreamReadConstraints.builder().maxStringLength(Integer.MAX_VALUE).build())
          .build());
    }
    return mapper;
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    init();
    converter.open(context);
  }

  void init() {
    if (converter == null) {
      // The document never leaves this function, so a row's size is no limit.
      converter = new DoclingConverter();
    }
    converter.init();
    if (parsing == null) {
      parsing = new Semaphore(PARSING);
    }
    if (pool == null) {
      pool = Executors.newFixedThreadPool(THREADS, runnable -> {
        Thread thread = new Thread(runnable, "docling-metrics");
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
   * @param downloadConn  the CONNECTION naming the artifact host
   * @param url           the filing's report, a public http(s) URL
   * @param userAgent     the User-Agent the service fetches the URL with, or NULL
   */
  public void eval(CompletableFuture<String[]> result, String doclingConn, String downloadConn, String url,
      String userAgent) {
    init();
    Instant until = Instant.now().plus(converter.deadline);
    try {
      pool.execute(() -> {
        try {
          String envelope = converter.call(doclingConn, downloadConn, url, userAgent, until);
          parsing.acquire();
          try {
            result.complete(fromEnvelope(envelope));
          } finally {
            parsing.release();
          }
        } catch (Throwable throwable) {  // NOPMD — the future must always complete with a value
          result.complete(problem(FAILED, throwable.getClass().getSimpleName() + ": " + throwable.getMessage()));
        }
      });
    } catch (RuntimeException rejected) {  // the pool is shut down: the function is closing
      result.complete(problem(FAILED, "the function is closing: " + rejected.getMessage()));
    }
  }

  /** The records of a {@link DoclingConverter} envelope. Never throws, never empty. */
  String[] fromEnvelope(String conversion) {
    if (conversion == null || conversion.isBlank()) {
      return problem(FAILED, "no conversion");
    }
    try {
      JsonNode envelope = mapper().readTree(conversion);
      if (!"success".equals(envelope.path("status").asText())) {
        return problem(FAILED, envelope.path("error").asText("the conversion failed"));
      }
      List<String> records = metrics(readDocument(envelope.path("content").asText("")));
      return records.isEmpty() ? problem(NOTHING_FOUND, "no statement with a known row label")
          : records.toArray(new String[0]);
    } catch (IOException notJson) {
      return problem(FAILED, "the conversion is not a DoclingDocument: " + notJson.getMessage());
    }
  }

  /** The one record of a filing that has no figures: why, as the {@code label}. */
  private String[] problem(String metric, String why) {
    ObjectNode record = mapper().createObjectNode();
    record.put("metric", metric);
    record.put("label", why == null ? "" : why.substring(0, Math.min(500, why.length())));
    record.putNull("value");
    record.putNull("prior_value");
    record.putNull("value_column");
    record.putNull("prior_column");
    record.putNull("scale");
    record.putNull("table");
    record.putNull("period_end");
    return new String[] {record.toString()};
  }

  /** A DoclingDocument as a tree, without {@link #DROPPED}. */
  JsonNode readDocument(String json) throws IOException {
    try (JsonParser parser = mapper().createParser(json)) {
      parser.nextToken();
      return read(parser);
    }
  }

  private JsonNode read(JsonParser parser) throws IOException {
    JsonToken token = parser.currentToken();
    if (token == JsonToken.START_OBJECT) {
      ObjectNode object = mapper().createObjectNode();
      while (parser.nextToken() == JsonToken.FIELD_NAME) {
        String name = parser.currentName();
        parser.nextToken();
        if (DROPPED.contains(name)) {
          parser.skipChildren();
        } else {
          object.set(name, read(parser));
        }
      }
      return object;
    }
    if (token == JsonToken.START_ARRAY) {
      ArrayNode array = mapper().createArrayNode();
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        array.add(read(parser));
      }
      return array;
    }
    return mapper().readTree(parser);
  }

  /** The records, from a parsed DoclingDocument. */
  List<String> metrics(JsonNode document) {
    List<String> out = new ArrayList<>();
    String period = periodEnd(document.path("texts"));
    int defaultScale = documentScale(document.path("texts"));
    JsonNode bodyChildren = document.path("body").path("children");
    Map<String, Integer> bodyIndex = bodyIndex(bodyChildren);
    JsonNode texts = document.path("texts");

    Map<String, ObjectNode> found = new LinkedHashMap<>();
    for (JsonNode table : document.path("tables")) {
      JsonNode grid = table.path("data").path("grid");
      int scale = 0;  // looked up when this table has something to report
      for (JsonNode row : grid) {
        if (row.isEmpty()) {
          continue;
        }
        String label = row.get(0).path("text").asText("").replaceAll("\\s+", " ").strip();
        if (label.isEmpty()) {
          continue;
        }
        String key = label.toLowerCase(Locale.ROOT).replaceAll("^[:*\\s]+|[:*\\s]+$", "");
        for (Map.Entry<String, Pattern> metric : METRICS.entrySet()) {
          if (found.containsKey(metric.getKey()) || !metric.getValue().matcher(key).matches()) {
            continue;
          }
          List<Figure> figures = figures(row);
          List<Double> values = new ArrayList<>();
          for (Figure figure : figures) {
            values.add(figure.value());
          }
          if (values.isEmpty()) {
            continue;
          }
          if (isEquityStatementRow(row, values)) {
            continue;  // its first figure is a dash, not the result
          }
          if (scale == 0) {
            scale = tableScale(table, bodyChildren, bodyIndex, texts, defaultScale);
          }
          ObjectNode record = mapper().createObjectNode();
          record.put("metric", metric.getKey());
          record.put("label", label);
          record.put("value", values.get(0));
          record.put("value_column", columnHead(grid, figures.get(0).position()));
          if (values.size() > 1) {
            record.put("prior_value", values.get(1));
            record.put("prior_column", columnHead(grid, figures.get(1).position()));
          } else {
            record.putNull("prior_value");
            record.putNull("prior_column");
          }
          record.put("scale", scale);
          record.put("table", table.path("self_ref").asText(""));
          if (period == null) {
            record.putNull("period_end");
          } else {
            record.put("period_end", period);
          }
          found.put(metric.getKey(), record);
        }
      }
    }
    for (ObjectNode record : found.values()) {
      out.add(record.toString());
    }
    return out;
  }

  /**
   * A statement of equity has a column per component — shares, par value,
   * paid-in capital — and a dash in the ones a line does not touch, so its "Net
   * loss" row starts with dashes and has many figures, and reading its first
   * would report a zero that is no result. Such a row is skipped. A row of
   * nothing but dashes is not one of those: a company with no revenue has it.
   */
  private static boolean isEquityStatementRow(JsonNode row, List<Double> values) {
    return values.size() >= 3 && values.get(0) == 0.0 && dashFirst(row)
        && values.stream().anyMatch(value -> value != 0.0);
  }

  private static boolean dashFirst(JsonNode row) {
    for (int i = 1; i < row.size(); i++) {
      String text = row.get(i).path("text").asText("").strip();
      if (text.isEmpty() || "$".equals(text)) {
        continue;
      }
      return "-".equals(text) || "—".equals(text) || "–".equals(text);
    }
    return false;
  }

  /**
   * The figures of a row after its label, as numbers: "(36,431)" is -36431, a
   * dash is zero, "$" and ")" cells are skipped. A cell spanning several
   * columns is repeated by Docling in each of them, so a cell is read once.
   */
  static List<Double> numbers(JsonNode row) {
    List<Double> out = new ArrayList<>();
    for (Figure figure : figures(row)) {
      out.add(figure.value());
    }
    return out;
  }

  /** A figure of a row and the position of its cell in the row, which is also its column in the head. */
  record Figure(double value, int position) { }

  static List<Figure> figures(JsonNode row) {
    List<Figure> out = new ArrayList<>();
    int lastStart = -1;
    boolean open = false;
    for (int i = 1; i < row.size(); i++) {
      JsonNode cell = row.get(i);
      int start = cell.path("start_col_offset_idx").asInt(i);
      if (start == lastStart) {
        continue;
      }
      lastStart = start;
      String text = cell.path("text").asText("").strip();
      if (text.isEmpty() || "$".equals(text) || ")".equals(text) || "%".equals(text) || ")%".equals(text)) {
        continue;
      }
      if ("-".equals(text) || "—".equals(text) || "–".equals(text)) {
        out.add(new Figure(0.0, i));
        continue;
      }
      if ("(".equals(text)) {  // the sign and the figure are in separate cells
        open = true;
        continue;
      }
      Matcher number = NUMBER.matcher(text);
      if (!number.matches()) {
        if (!out.isEmpty()) {
          break;  // text after the figures: a footnote, a note reference
        }
        continue;
      }
      double value = Double.parseDouble(number.group(1).replace(",", ""));
      out.add(new Figure(text.startsWith("(") || open ? -value : value, i));
      open = false;
    }
    return out;
  }

  /** The head is at most this many rows: the periods are in the first few. */
  static final int HEAD_ROWS = 4;

  /**
   * What the head of a table says above a column: "Three Months Ended
   * September 30, 2026", "December 31, 2025". The head is the rows at the top
   * that have no label or no figure ("(in thousands)" is one); the column is
   * the position of the figure in its row, since Docling repeats a spanning cell
   * in every column it spans. Without it the second figure of a row is "the
   * other column" and nothing says which period that is: a balance sheet of a
   * 10-Q compares with the last year-end, an income statement with last year's
   * quarter.
   */
  static String columnHead(JsonNode grid, int position) {
    List<String> parts = new ArrayList<>();
    for (int r = 0; r < Math.min(HEAD_ROWS, grid.size()); r++) {
      JsonNode row = grid.get(r);
      if (isBodyRow(row)) {
        break;
      }
      if (position >= row.size()) {
        continue;
      }
      String text = row.get(position).path("text").asText("").replaceAll("\\s+", " ").strip();
      if (!text.isEmpty() && !"$".equals(text) && scaleOf(text) == 0 && !parts.contains(text)) {
        parts.add(text);
      }
    }
    String head = String.join(" ", parts);
    return head.substring(0, Math.min(100, head.length()));
  }

  /**
   * The first row of a table's body: a label and figures that are not years. A 10-Q's income statement
   * has a head of two lines, "Three months ended September 30," over "(in thousands) | 2026 | 2025", and
   * the second one has a label and numbers too.
   */
  private static boolean isBodyRow(JsonNode row) {
    String label = row.isEmpty() ? "" : row.get(0).path("text").asText("").strip();
    if (label.isEmpty() || scaleOf(label) != 0) {
      return false;
    }
    List<Double> values = numbers(row);
    return !values.isEmpty() && !values.stream().allMatch(DoclingMetrics::isYear);
  }

  private static boolean isYear(double value) {
    return value == Math.rint(value) && value >= 1900 && value <= 2100;
  }

  /** What a statement says its figures are in: the table's head, the text above it, the document. */
  private static int tableScale(JsonNode table, JsonNode children, Map<String, Integer> bodyIndex, JsonNode texts,
      int fallback) {
    JsonNode grid = table.path("data").path("grid");
    for (int r = 0; r < Math.min(4, grid.size()); r++) {
      for (JsonNode cell : grid.get(r)) {
        int scale = scaleOf(cell.path("text").asText(""));
        if (scale != 0) {
          return scale;
        }
      }
    }
    // The few items before the table, in reading order. Only when the table is a child of the body.
    Integer at = bodyIndex.get(table.path("self_ref").asText(""));
    if (at != null) {
      for (int i = at - 1; i >= Math.max(0, at - 4); i--) {
        String ref = children.get(i).path("$ref").asText("");
        if (ref.startsWith("#/texts/")) {
          int scale = scaleOf(texts.path(Integer.parseInt(ref.substring("#/texts/".length()))).path("text").asText(""));
          if (scale != 0) {
            return scale;
          }
        }
      }
    }
    return fallback;
  }

  private static Map<String, Integer> bodyIndex(JsonNode bodyChildren) {
    Map<String, Integer> index = new LinkedHashMap<>();
    for (int i = 0; i < bodyChildren.size(); i++) {
      index.put(bodyChildren.get(i).path("$ref").asText(""), i);
    }
    return index;
  }

  private static int scaleOf(String text) {
    Matcher match = SCALE.matcher(text);
    if (!match.find()) {
      return 0;
    }
    return "thousands".equalsIgnoreCase(match.group(1)) ? 1_000 : 1_000_000;
  }

  /** The first "in thousands" / "in millions" in the document; 1 when it never says. */
  private static int documentScale(JsonNode texts) {
    for (JsonNode text : texts) {
      int scale = scaleOf(text.path("text").asText(""));
      if (scale != 0) {
        return scale;
      }
    }
    return 1;
  }

  /**
   * The report's period on the cover, as an ISO date. Covers say it three ways:
   * "For the quarterly period ended September 30, 2026" in one text; "For the
   * fiscal year ended" and the date in the next; or the date alone.
   */
  static String periodEnd(JsonNode texts) {
    String standalone = null;
    String previous = "";
    int n = 0;
    for (JsonNode node : texts) {
      if (n++ >= COVER_TEXTS) {
        break;
      }
      String text = node.path("text").asText("").strip();
      Matcher inline = PERIOD.matcher(text);
      String date = inline.find() ? parseDate(inline.group(1)) : null;
      if (date == null && ENDED.matcher(previous).find()) {
        date = parseDate(text);
      }
      if (date != null) {
        return date;
      }
      if (standalone == null && DATE_ONLY.matcher(text).matches()) {
        standalone = parseDate(text);
      }
      previous = text;
    }
    return standalone;
  }

  private static String parseDate(String text) {
    if (text == null) {
      return null;
    }
    Matcher match = DATE.matcher(text);
    while (match.find()) {
      try {
        String date = match.group(1).replace(".", "").replace(",", " ").replaceAll("\\s+", " ");
        return LocalDate.parse(capitalised(date), LONG_DATE).toString();
      } catch (RuntimeException notADate) {
        // "Sept. 30, 2026" and friends: try the next one
      }
    }
    return null;
  }

  private static String capitalised(String date) {
    String lower = date.toLowerCase(Locale.ROOT);
    return Character.toUpperCase(lower.charAt(0)) + lower.substring(1);
  }
}
