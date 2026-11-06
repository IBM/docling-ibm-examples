// Copyright IBM Corp. 2026
// SPDX-License-Identifier: Apache-2.0
package ai.docling.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;

import org.junit.jupiter.api.Test;

/**
 * Against the worked example in AWS's own SigV4 documentation, "Authenticating
 * Requests: Using Query Parameters" — the one fixed point a hand-written signer
 * can be checked against without an account.
 */
class S3PresignTest {

  static final String ID = "AKIAIOSFODNN7EXAMPLE";
  static final String SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
  static final Instant WHEN = Instant.parse("2013-05-24T00:00:00Z");

  @Test
  void matchesTheAwsDocumentationExample() {
    String url = S3Presign.presign("https://examplebucket.s3.amazonaws.com", ID, SECRET, null,
        "test.txt", 86400, null, WHEN);
    assertEquals("https://examplebucket.s3.amazonaws.com/test.txt"
        + "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
        + "&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request"
        + "&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host"
        + "&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404", url);
  }

  /** The second fixed point: botocore's S3SigV4QueryAuth, same key pair and clock, an awkward key. */
  @Test
  void matchesBotocoreForAKeyThatNeedsEncoding() {
    String url = S3Presign.presign("https://b.s3.eu-west-1.amazonaws.com", ID, SECRET, null,
        "inbox/my report (v2)+ü.pdf", 600, null, WHEN);
    assertEquals("https://b.s3.eu-west-1.amazonaws.com/inbox/my%20report%20%28v2%29%2B%C3%BC.pdf"
        + "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
        + "&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Feu-west-1%2Fs3%2Faws4_request"
        + "&X-Amz-Date=20130524T000000Z&X-Amz-Expires=600&X-Amz-SignedHeaders=host"
        + "&X-Amz-Signature=e5aae0227837cc14c7a6419e4936a25e72310f124d540a2fae13e6328304eefb", url);
  }

  @Test
  void pathStyleKeepsTheBucketInThePath() {
    String url = S3Presign.presign("https://s3.us-west-2.amazonaws.com/my.dotted.bucket/", ID, SECRET, null,
        "a.pdf", 600, null, WHEN);
    assertTrue(url.startsWith("https://s3.us-west-2.amazonaws.com/my.dotted.bucket/a.pdf?"), url);
    assertTrue(url.contains("%2Fus-west-2%2F"), url);
  }

  @Test
  void carriesASessionToken() {
    String url = S3Presign.presign("https://b.s3.us-east-1.amazonaws.com", ID, SECRET, "tok/en+=",
        "a.pdf", 600, null, WHEN);
    assertTrue(url.contains("&X-Amz-Security-Token=tok%2Fen%2B%3D&X-Amz-SignedHeaders=host&"), url);
  }

  @Test
  void readsTheRegionFromTheHost() {
    assertEquals("us-east-1", S3Presign.regionOf("bucket.s3.amazonaws.com"));
    assertEquals("eu-central-1", S3Presign.regionOf("bucket.s3.eu-central-1.amazonaws.com"));
    assertEquals("ap-southeast-2", S3Presign.regionOf("s3.ap-southeast-2.amazonaws.com"));
    assertEquals("us-gov-west-1", S3Presign.regionOf("bucket.s3-us-gov-west-1.amazonaws.com"));
    assertEquals("us-east-2", S3Presign.regionOf("bucket.s3.dualstack.us-east-2.amazonaws.com"));
    assertThrows(IllegalArgumentException.class, () -> S3Presign.regionOf("minio.example.com"));
  }

  @Test
  void anExplicitRegionWinsForAnotherStore() {
    String url = S3Presign.presign("http://minio.local:9000/docs", ID, SECRET, null, "a.pdf", 600, "us-east-1", WHEN);
    assertTrue(url.startsWith("http://minio.local:9000/docs/a.pdf?"), url);
  }

  @Test
  void theTokenIsIdColonSecret() {
    String[] parts = S3Presign.parseToken("c", ID + ":" + SECRET);
    assertEquals(ID, parts[0]);
    assertEquals(SECRET, parts[1]);
    assertNull(parts[2]);
    assertEquals("session", S3Presign.parseToken("c", "a:b:session")[2]);
    assertThrows(IllegalStateException.class, () -> S3Presign.parseToken("c", ID));
    assertThrows(IllegalStateException.class, () -> S3Presign.parseToken("c", ""));
  }

  @Test
  void aMissingKeyIsNullNotAnError() {
    assertNull(new S3Presign().eval("c", null));
    assertNull(new S3Presign().eval("c", ""));
  }

  @Test
  void anUnboundConnectionSaysSo() {
    IllegalStateException error = assertThrows(IllegalStateException.class, () -> new S3Presign().eval("c", "a.pdf"));
    assertTrue(error.getMessage().contains("USING CONNECTIONS"), error.getMessage());
  }

  @Test
  void refusesAnExpiryAwsWouldReject() {
    assertThrows(IllegalArgumentException.class, () -> S3Presign.presign("https://b.s3.amazonaws.com", ID, SECRET,
        null, "a", 7 * 24 * 3600 + 1, null, WHEN));
  }
}
