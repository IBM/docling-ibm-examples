package ai.docling.confluent;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * A presigned GET URL for one object in a private S3 bucket.
 *
 * <pre>
 *   dc_presign(bucket_connection, key [, ttl_seconds [, region]])  -&gt;  https://…?X-Amz-Signature=…
 * </pre>
 *
 * <h2>Why it exists</h2>
 *
 * Example 03's documents are in a private bucket, and {@link DoclingConvert}
 * hands Docling a URL to fetch. A presigned URL is the smallest thing that
 * turns one into the other: a URL that is its own credential, for one object,
 * for a few minutes. Docling fetches it; nothing else in Flink touches the
 * bucket.
 *
 * <h2>No AWS SDK, and no request</h2>
 *
 * Presigning is AWS Signature Version 4 in query-string form — a pure
 * computation over the key, the secret, the region and the clock. Nothing is
 * sent to AWS. About a hundred lines of HMAC-SHA256 here, against an SDK that
 * is tens of megabytes and wants credential providers and a region chain a UDF
 * cannot give it.
 *
 * <h2>The connection</h2>
 *
 * The credential lives in a Flink {@code CONNECTION}, never in a statement. A
 * connection exposes exactly two fields to a function, so they carry:
 *
 * <pre>
 *   endpoint   https://&lt;bucket&gt;.s3.&lt;region&gt;.amazonaws.com     virtual-hosted style
 *              https://s3.&lt;region&gt;.amazonaws.com/&lt;bucket&gt;     path style (a bucket with dots)
 *   token      &lt;access-key-id&gt;:&lt;secret-access-key&gt;[:&lt;session-token&gt;]
 * </pre>
 *
 * The region is read from the endpoint's host; pass it explicitly for an
 * S3-compatible store whose host does not say.
 *
 * <h2>Failures</h2>
 *
 * A row with no key is NULL — filter it. A connection that is missing or
 * malformed is a <em>configuration</em> error, the same for every row, so it
 * throws: the statement fails at the first record with the reason in its
 * detail, rather than turning every document into an identical failure row.
 */
public class S3Presign extends ScalarFunction {

  private static final long serialVersionUID = 1L;

  /** Long enough for Docling to fetch the object; short, because the URL lands on a topic. */
  static final int DEFAULT_TTL_SECONDS = 600;
  /** SigV4's own ceiling for a presigned URL. */
  static final int MAX_TTL_SECONDS = 7 * 24 * 3600;

  private static final DateTimeFormatter AMZ_DATE =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
  /** {@code <bucket>.s3.<region>.amazonaws.com}, {@code s3.<region>…}, {@code s3-<region>…}, {@code s3.dualstack.<region>…}. */
  private static final Pattern REGION_IN_HOST =
      Pattern.compile("(?:^|\\.)s3[.-](?:dualstack\\.)?([a-z]{2}(?:-[a-z]+)+-\\d+)\\.amazonaws\\.com$");

  private transient FunctionContext context;

  @Override
  public void open(FunctionContext context) throws Exception {
    this.context = context;
  }

  /** A new signature on every call: the clock is an input. */
  @Override
  public boolean isDeterministic() {
    return false;
  }

  public String eval(String connection, String key) {
    return eval(connection, key, DEFAULT_TTL_SECONDS, null);
  }

  public String eval(String connection, String key, Integer ttlSeconds) {
    return eval(connection, key, ttlSeconds, null);
  }

  /**
   * @param connection  the Flink CONNECTION holding the bucket endpoint and the key pair
   * @param key         the object key, decoded — {@code inbox/my report.pdf}, not {@code inbox/my+report.pdf}
   * @param ttlSeconds  how long the URL works for; NULL for {@link #DEFAULT_TTL_SECONDS}
   * @param region      the signing region; NULL to read it from the endpoint
   */
  public String eval(String connection, String key, Integer ttlSeconds, String region) {
    if (key == null || key.isEmpty()) {
      return null;
    }
    String endpoint = jobParameter(connection, "endpoint");
    String token = jobParameter(connection, "token");
    if (endpoint == null || endpoint.isBlank()) {
      throw new IllegalStateException("connection '" + connection + "' has no endpoint. Is the function bound "
          + "to it with USING CONNECTIONS, and is that the connection's name?");
    }
    String[] credential = parseToken(connection, token);
    return presign(endpoint, credential[0], credential[1], credential[2], key,
        ttlSeconds == null ? DEFAULT_TTL_SECONDS : ttlSeconds, region, Instant.now());
  }

  /** {@code id:secret} or {@code id:secret:session-token} -> {id, secret, session-token or null}. */
  static String[] parseToken(String connection, String token) {
    String[] parts = token == null ? new String[0] : token.split(":", 3);
    if (parts.length < 2 || parts[0].isBlank() || parts[1].isBlank()) {
      throw new IllegalStateException("connection '" + connection + "' must carry its token as "
          + "<access-key-id>:<secret-access-key>[:<session-token>]");
    }
    return new String[] {parts[0].trim(), parts[1].trim(), parts.length == 3 && !parts[2].isBlank() ? parts[2].trim() : null};
  }

  /**
   * The whole signature — what the tests drive.
   *
   * @param endpoint  {@code https://bucket.s3.region.amazonaws.com}, or a path-style {@code https://host/bucket}
   */
  static String presign(String endpoint, String accessKeyId, String secretAccessKey, String sessionToken,
      String key, int ttlSeconds, String region, Instant now) {
    if (ttlSeconds < 1 || ttlSeconds > MAX_TTL_SECONDS) {
      throw new IllegalArgumentException("ttl_seconds must be between 1 and " + MAX_TTL_SECONDS + ", got " + ttlSeconds);
    }
    URI base = URI.create(endpoint.trim());
    String host = base.getHost();
    if (host == null) {
      throw new IllegalArgumentException("not an endpoint URL: " + endpoint);
    }
    String hostHeader = base.getPort() == -1 ? host : host + ":" + base.getPort();
    String signingRegion = region == null || region.isBlank() ? regionOf(host) : region.trim();

    // Path style keeps the bucket in the path; virtual-hosted has none. Either
    // way the object key follows, each segment encoded, the slashes kept.
    String prefix = base.getRawPath() == null ? "" : base.getRawPath().replaceAll("/+$", "");
    String canonicalUri = prefix + "/" + encode(key, false);

    String amzDate = AMZ_DATE.format(now);
    String date = amzDate.substring(0, 8);
    String scope = date + "/" + signingRegion + "/s3/aws4_request";

    Map<String, String> query = new TreeMap<>();  // sorted by name: the canonical order
    query.put("X-Amz-Algorithm", "AWS4-HMAC-SHA256");
    query.put("X-Amz-Credential", accessKeyId + "/" + scope);
    query.put("X-Amz-Date", amzDate);
    query.put("X-Amz-Expires", Integer.toString(ttlSeconds));
    if (sessionToken != null) {
      query.put("X-Amz-Security-Token", sessionToken);
    }
    query.put("X-Amz-SignedHeaders", "host");
    StringBuilder canonicalQuery = new StringBuilder();
    for (Map.Entry<String, String> entry : query.entrySet()) {
      if (canonicalQuery.length() > 0) {
        canonicalQuery.append('&');
      }
      canonicalQuery.append(encode(entry.getKey(), true)).append('=').append(encode(entry.getValue(), true));
    }

    String canonicalRequest = "GET\n" + canonicalUri + "\n" + canonicalQuery + "\nhost:" + hostHeader
        + "\n\nhost\nUNSIGNED-PAYLOAD";
    String stringToSign = "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex(sha256(canonicalRequest));

    byte[] signingKey = hmac(("AWS4" + secretAccessKey).getBytes(StandardCharsets.UTF_8), date);
    signingKey = hmac(signingKey, signingRegion);
    signingKey = hmac(signingKey, "s3");
    signingKey = hmac(signingKey, "aws4_request");
    String signature = hex(hmac(signingKey, stringToSign));

    return base.getScheme() + "://" + hostHeader + canonicalUri + "?" + canonicalQuery
        + "&X-Amz-Signature=" + signature;
  }

  /** The region in an AWS S3 host name; us-east-1 for the global {@code s3.amazonaws.com}. */
  static String regionOf(String host) {
    String lower = host.toLowerCase(Locale.ROOT);
    Matcher matcher = REGION_IN_HOST.matcher(lower);
    if (matcher.find()) {
      return matcher.group(1);
    }
    if (lower.equals("s3.amazonaws.com") || lower.endsWith(".s3.amazonaws.com")) {
      return "us-east-1";
    }
    throw new IllegalArgumentException("no region in the endpoint host " + host
        + " — pass it as dc_presign's fourth argument");
  }

  /**
   * RFC 3986 percent-encoding, as SigV4 defines it: everything but
   * {@code A-Z a-z 0-9 - _ . ~} is encoded, a space is {@code %20} (never
   * {@code +}), and {@code /} is kept in a path but not in a query value.
   */
  static String encode(String value, boolean encodeSlash) {
    StringBuilder out = new StringBuilder();
    for (byte b : value.getBytes(StandardCharsets.UTF_8)) {
      int c = b & 0xff;
      if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
          || c == '-' || c == '_' || c == '.' || c == '~' || (c == '/' && !encodeSlash)) {
        out.append((char) c);
      } else {
        out.append('%').append(Character.toUpperCase(Character.forDigit(c >> 4, 16)))
            .append(Character.toUpperCase(Character.forDigit(c & 0xf, 16)));
      }
    }
    return out.toString();
  }

  private static byte[] hmac(byte[] key, String data) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(key, "HmacSHA256"));
      return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
    } catch (GeneralSecurityException exception) {
      throw new IllegalStateException("HmacSHA256 is not available", exception);
    }
  }

  private static byte[] sha256(String data) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(data.getBytes(StandardCharsets.UTF_8));
    } catch (GeneralSecurityException exception) {
      throw new IllegalStateException("SHA-256 is not available", exception);
    }
  }

  private static String hex(byte[] bytes) {
    return HexFormat.of().formatHex(bytes);
  }

  private String jobParameter(String connection, String field) {
    if (connection == null || connection.isBlank() || context == null) {
      return null;
    }
    return context.getJobParameter(connection + "." + field, null);
  }
}
