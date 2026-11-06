# Example 03 — the AWS side

Example 03 reads documents from a private S3 bucket. Before `./setup.sh` can
run, four things have to exist in your AWS account. [`scripts/aws.sh`](scripts/aws.sh)
creates them. This page explains what they are, so you can create them by hand
instead, or check what the script did.

| | what | why |
|---|---|---|
| **bucket** | where the documents are; private | the source |
| **SQS queue** | receives one message per upload | the connector's input |
| **bucket notification** | `s3:ObjectCreated:*` under a prefix → the queue | turns an upload into a message |
| **IAM user + access key** | can read the prefix and the queue, nothing else | the key Confluent holds |

```
 you ──upload──▶ s3://bucket/inbox/report.pdf
                        │ notification
                        ▼
                    SQS queue ◀── scripts/backfill.py (objects already there)
                        │
                        ▼  reader key
          Confluent SQS Source connector ──▶ objects topic ──▶ Flink
                                                                │ dc_presign, reader key
                                                                ▼
                                     Docling ◀── presigned GET URL, valid 10 min
```

Everything is named after `NS` (from `05-confluent-cloud/.env`), so two people sharing
an AWS account don't collide. `scripts/aws.sh info` prints the names before
anything is created.

## Before you start

- The **aws CLI**, logged in as someone who can create S3 buckets, SQS queues
  and IAM users: `aws sts get-caller-identity` should answer. The script runs as
  you (your default credentials, or `AWS_PROFILE`). None of your own
  credentials are written anywhere.
- `05-confluent-cloud/.env` filled in (`./account.sh check` passes). The script reads
  `NS` and uses `CC_REGION` as its default AWS region.

## The short way

```bash
scripts/aws.sh create                  # or: --bucket my-existing-bucket --prefix docs/
scripts/aws.sh samples                 # three public PDFs, uploaded under the prefix
scripts/aws.sh check                   # everything there, and the reader key works
```

`create` is safe to run again: it reuses what exists and updates the queue
policy, notification and IAM policy in place. It writes what it made into this
example's `.env` (gitignored), which is where `./setup.sh` reads it from:

| | |
|---|---|
| `S3_BUCKET`, `S3_REGION`, `S3_PREFIX` | where the documents are (default prefix `inbox/`) |
| `S3_BUCKET_OWNED` | `yes` if the script created the bucket; only then may `destroy --bucket` delete it |
| `S3_NOTIFIED_AT` | when the notification started; the backfill sends only older objects |
| `SQS_QUEUE_URL`, `SQS_QUEUE_ARN` | the queue |
| `AWS_READER_KEY_ID`, `AWS_READER_SECRET` | the reader's access key |

The names are deliberately not the standard `AWS_ACCESS_KEY_ID` /
`AWS_REGION`. This example's scripts export everything in `.env`, and a standard
name would quietly switch your own aws CLI to the reader's key.

## The long way: by hand

These are the commands `create` runs, with the names it would use for
`NS=demo`, account `123456789012` and region `eu-west-1`. Substitute your
own.

### 1. The bucket

Any bucket you own works; it does not need to be new. If you create one, keep it
private. Presigned URLs work through Block Public Access, and nothing in this
example needs the bucket to be public.

```bash
aws s3api create-bucket --bucket demo-ex03-docling-123456789012 \
  --create-bucket-configuration LocationConstraint=eu-west-1    # omit for us-east-1
aws s3api put-public-access-block --bucket demo-ex03-docling-123456789012 \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

The queue must be in the **same region as the bucket**, because S3 won't
notify a queue in another region. The bucket's region does not need to match
your Confluent cluster's.

### 2. The queue, and who may write to it

A standard queue (S3 can't notify a FIFO queue), with long polling so the
connector's empty receives are cheap:

```bash
aws sqs create-queue --queue-name demo-ex03-objects --attributes ReceiveMessageWaitTimeSeconds=20
```

Its access policy lets **S3, for this bucket, in this account** send to it. The
two conditions are what stop any other bucket in AWS from filling your queue:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "s3.amazonaws.com" },
    "Action": "sqs:SendMessage",
    "Resource": "arn:aws:sqs:eu-west-1:123456789012:demo-ex03-objects",
    "Condition": {
      "ArnEquals":    { "aws:SourceArn": "arn:aws:s3:::demo-ex03-docling-123456789012" },
      "StringEquals": { "aws:SourceAccount": "123456789012" }
    }
  }]
}
```

Set it as the queue's `Policy` attribute (`aws sqs set-queue-attributes`). It
has to be in place before step 3: S3 checks that it may send to the queue when
you add the notification.

### 3. The notification

```json
{
  "QueueConfigurations": [{
    "Id": "demo-ex03",
    "QueueArn": "arn:aws:sqs:eu-west-1:123456789012:demo-ex03-objects",
    "Events": ["s3:ObjectCreated:*"],
    "Filter": { "Key": { "FilterRules": [{ "Name": "prefix", "Value": "inbox/" }] } }
  }]
}
```

> **`put-bucket-notification-configuration` replaces the bucket's entire
> notification configuration.** On a bucket that already has notifications,
> read the current one first (`get-bucket-notification-configuration`), add
> this entry, and write the whole thing back. The script does exactly that and
> touches only the entry whose `Id` is yours. S3 also refuses two rules for the
> same events with overlapping prefixes; if that happens, pick another prefix.

Adding it sends one `s3:TestEvent` message to the queue. The Flink view drops
it.

### 4. The reader: an IAM user with one narrow policy

One user covers both of the things that need AWS access on the Confluent side:

- the **SQS Source connector**, which receives and deletes messages;
- **`dc_presign`**, whose presigned URLs can do exactly what the signing key can
  and no more. So the key needs `s3:GetObject` on the prefix, and that's all.

```bash
aws iam create-user --user-name demo-ex03-confluent
aws iam put-user-policy --user-name demo-ex03-confluent --policy-name docling-ex03-reader --policy-document file://reader.json
aws iam create-access-key --user-name demo-ex03-confluent
```

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::demo-ex03-docling-123456789012/inbox/*" },
    { "Effect": "Allow",
      "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility",
                 "sqs:GetQueueAttributes", "sqs:GetQueueUrl"],
      "Resource": "arn:aws:sqs:eu-west-1:123456789012:demo-ex03-objects" }
  ]
}
```

No `s3:ListBucket` and no `sqs:SendMessage`. The reader can't see which
objects exist, and can't put anything in the queue. Listing the bucket and
writing to the queue are the backfill's jobs, and the backfill runs as you.

Write the values from steps 1–4 into `examples/03-docling-in-flink-s3/.env`
with the names in the table above. Set `S3_NOTIFIED_AT` to when you added the
notification, in UTC (`2026-10-06T12:00:00Z`).

## Where the key ends up on the Confluent side

`./setup.sh` puts the reader key in two places, and nowhere else:

- the **connector's config** (`terraform/main.tf`): `aws.access.key.id`
  and `aws.secret.key.id`. Confluent stores these as passwords.
- the **bucket connection** (`flink/05_connection_bucket.sql`), as its token
  `<id>:<secret>`. A Flink connection exposes only `endpoint` and `token` to a
  UDF, so the pair travels as one string. `SHOW CREATE CONNECTION` shows it
  redacted.

## Objects that were there before: the backfill

Notifications cover only objects created after step 3. For the rest:

```bash
scripts/aws.sh backfill --dry-run      # what it would send
scripts/aws.sh backfill                # one message per object under the prefix
```

This lists the bucket and sends each object **to the queue** as an S3-shaped
event, so the connector delivers it like a live upload. Details are in
[`scripts/backfill.py`](scripts/backfill.py).

It sends only objects **older than the notification**: `S3_NOTIFIED_AT`,
recorded by `create`, plus five minutes' margin. Newer objects were announced
already, and Flink doesn't deduplicate (`flink/22_view_objects.sql` explains
why), so sending them again would convert them again. `--since <UTC time>`
sends what changed after a given moment instead (for uploads announced while no
convert statement was deployed), and `--all` sends everything.

Run it after `./pipeline.sh deploy`. The convert statement reads from the
moment it was deployed, so anything sent earlier is skipped. **Every message is
one billed Docling conversion**, and running the backfill twice converts
everything twice. Use `--dry-run` first, and narrow it with `--prefix`,
`--suffix .pdf` or `--limit`.

## What it costs

All of this is small next to the Confluent side (the connector and two Flink
statements bill by the hour) and the Docling conversions (per page).

- **SQS**: the connector long-polls, about 3 requests a minute with an idle
  queue. That's well inside the free tier, otherwise cents a month.
- **S3**: storage for your documents, plus one GET per conversion, made by
  Docling.
- **IAM**: free.

## Tearing it down

The Confluent side first, because the connector fails once its queue is gone:

```bash
./setup.sh destroy                   # connector, statements, functions, connections
scripts/aws.sh destroy               # notification entry, queue, IAM user and its keys
scripts/aws.sh destroy --bucket      # ...and empty + delete the bucket, only if the script made it
```

A bucket you named with `--bucket` is never deleted. Only the script's own
notification entry is removed from it.

## For production

The script optimizes for one command and a clean teardown. In production:

- **Connector auth through an IAM role** instead of an access key: Confluent's
  *provider integration* (`authentication.method = IAM Roles`,
  `provider.integration.id`). Nothing long-lived to rotate.
- **Short-lived credentials for presigning.** `dc_presign` accepts
  `<id>:<secret>:<session-token>`. The catch is that a connection can't refresh
  itself, so something has to rotate it. Or skip presigning entirely: Docling
  has its own `s3` source kind and can read the bucket with credentials you give
  it. That's simpler, but it takes the bucket out of Flink's hands, which is
  the opposite of what this example demonstrates.
- **SSE-KMS buckets**: the reader also needs `kms:Decrypt` on the key, or every
  presigned GET is a 403.
- **A dead-letter queue** on the SQS queue, so a message the connector can't
  deliver doesn't retry forever.

## When something doesn't work

| symptom | likely cause |
|---|---|
| `create`: S3 refuses the notification | another rule on the bucket covers `ObjectCreated` under an overlapping prefix; choose another `--prefix` |
| `check`: the key does not authenticate | a new IAM key takes a few seconds to work; run `check` again |
| connector `FAILED` right after setup | the key isn't valid yet (as above): `./setup.sh apply` applies the config again. Or the queue was recreated since: `scripts/aws.sh create` updates the policy |
| uploads, but nothing on the objects topic | `scripts/aws.sh check` shows the messages waiting in the queue: the connector isn't running (`./pipeline.sh status`). Zero waiting: the upload is outside the prefix |
| conversion failure `HTTP 403` | the key in the event wasn't decoded (spaces and `+`), the object is outside the reader's prefix, or the bucket uses SSE-KMS |
| conversion failure mentioning expiry | Docling fetched after `PRESIGN_TTL` (default 600 s); raise it in `.env.local` and redeploy |
