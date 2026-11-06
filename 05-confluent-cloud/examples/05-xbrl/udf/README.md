# xbrl-udf

Example 05's Java UDF artifact. One registered function:

| function | SQL name | what it is |
|---|---|---|
| `DoclingMetrics` | `${NS}_ex05_metrics` (`dc_metrics`) | converts a filing with Docling and reads its headline figures — revenue, net income, total assets, … — from the tables of the DoclingDocument. `ARRAY<STRING>`, one JSON object per metric. |

    src/main/java/ai/docling/confluent/xbrl/DoclingMetrics.java    the function, and the reading of the tables
    src/main/java/ai/docling/confluent/xbrl/DoclingConverter.java  the Docling calls (submit, poll, result, download)

    ./build.sh          -> target/docling-xbrl-udf.jar, ready to upload
    ./build.sh test     -> the unit tests, without a cluster

It is in this example, not in `common/udf-java`, because nothing else uses it.
`DoclingConverter` makes the same calls as `dc_convert` there, with two
differences this example needs: the `User-Agent` the Docling service fetches the
URL with (the SEC answers 403 without one), and no cap on the size of the
document, since it never leaves the function and a Confluent Cloud row (4 MB)
is no limit. Why Java, and why the function is asynchronous with a 45 s
deadline: see [`common/udf-java/README.md`](../../../common/udf-java/README.md).
