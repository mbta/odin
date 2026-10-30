# Cubic mover Lambda

`scripts/cubic_mover_lambda.py` is deployed alone, using Python 3.13 and Lambda's
boto3. The handler is `cubic_mover_lambda.lambda_handler`. It moves selected Qlik
CSV/DFM pairs without PostgreSQL, Glue, transformations, or the legacy error mover.

## Tables and locations

Populate `TABLE_PREFIXES` at the top of the script with **table names only**:

```python
TABLE_PREFIXES: list[str] = [
    "EDW.WHATEVER",
]
```

The shipped list is empty: nothing is moved until it is populated and deployed.
An entry allows exactly `cubic/ods_qlik/EDW.WHATEVER/` and its `__ct/` counterpart.
Other tables, DMAP paths, and non-CSV notifications are logged and ignored.
There is no environment or S3 override for the list.

| Environment variable | Value |
| --- | --- |
| `S3_BUCKET_INCOMING` | Required bucket name, without `s3://` |
| `S3_BUCKET_ARCHIVE` | Required bucket name, without `s3://` |
| `S3_BUCKET_PREFIX_INCOMING` | Optional prefix before `cubic/ods_qlik/`; empty or ending in `/` |
| `S3_BUCKET_PREFIX_ARCHIVE` | Optional prefix before `cubic/ods_qlik/`; empty or ending in `/` |

Same-bucket testing requires disjoint prefixes, for example `mover_test/incoming/`
and `mover_test/archive/`.

## Archive layout

The reference is `mbta/data_platform` at
[`445d5dff55d34a165670cce8819da1cbef3f96e8`](https://github.com/mbta/data_platform/tree/445d5dff55d34a165670cce8819da1cbef3f96e8),
particularly `workers/archive.ex` and `schema/cubic_load.ex`.

`cubic/ods_qlik/EDW.WHATEVER/LOAD00000001.csv.gz` becomes
`cubic/ods_qlik/EDW.WHATEVER/snapshot=YYYYMMDDTHHMMSSZ/LOAD00000001.csv.gz`.
The same-basename DFM goes beside it.

The snapshot timestamp is the first LOAD CSV's S3 LastModified, truncated to UTC
seconds, not notification time or DFM write time. Both `LOAD00000001.csv.gz` and
the historical sample name `LOAD1.csv.gz` are recognized. Other chunks and CDC
files use the latest base-table snapshot no later than their own S3 LastModified.
CDC files retain their `__ct` directory but share the base table's snapshot.

Snapshot discovery checks incoming first-LOAD files and archive partition names.
It also checks Odin's relocated copies under these prefixes, followed by the
configured archive prefix and original Qlik path:

- `odin/archive/cubic_qlik/processed/`
- `odin/archive/cubic_qlik/ignored/`
- `odin/error/cubic_qlik/`

Retain these partitions: they are the snapshot state. The producer must upload a
generation's first LOAD before its later files and must supply a coherent
CSV/DFM pair before reusing their keys for another generation. A removed first
LOAD with no retained partition cannot be reconstructed by this script.

## Copy and failure policy

The handler checks the notification against the current CSV's ETag, size,
version ID and modification time, requires both nonempty sources, and refuses
any existing destination CSV **or** DFM, including relocated copies. It then
copies both files, checks their destination sizes, and only then deletes sources.
Copies pin source versions where available and use source ETag conditions;
deletes target inspected versions and use ETag conditions.

**S3 user metadata and source content headers are intentionally not retained.**
Copies use `MetadataDirective="REPLACE"` with empty metadata. There are no custom
receipts, hashes, or persistent per-event records. This differs from the legacy
mover's default S3 metadata preservation. DFM file **contents** are still copied
unchanged; they are not S3 object metadata. Large files use managed multipart
copy without downloading CSV bytes into Lambda.

Boto3 retries transient API failures. The script does **not** recover partial
moves across invocations or acknowledge missing sources as already archived.
Any missing/replaced source, missing snapshot, existing destination, copy
verification failure, or copy/delete error is logged and fails the invocation.
Other records in the same invocation are still attempted.

Consequences of intentionally omitting durable receipts:

- A duplicate notification after a successful move also fails for reconciliation.
- If the CSV copy succeeds and the DFM copy fails, both sources remain. A later
  invocation refuses the existing CSV rather than completing the pair.
- A failure between deletes leaves a partial source pair. It is not cleaned up
  automatically.
- Replaying an entire batch can report failures for records already completed.
- A request failure before any mutation can be retried safely after it is fixed.

An error does not undo prior copies/deletes and does not disable the Lambda.
Before redrive, inspect source versions and destination **contents**, determine
whether the pair was fully archived, and either complete cleanup manually or
restore a consistent input state and resolve conflicting destinations. Do not
delete archive objects merely because a notification failed.

## Infrastructure and cutover

The deployment workflow validates and uploads code; it does not configure these
requirements:

1. Use direct S3 `s3:ObjectCreated:*` notifications, filtered to the incoming Qlik
   prefix and `.csv.gz`. SQS/SNS/EventBridge envelopes are not supported.
2. Set reserved concurrency to **1** and make this the only archive writer.
   Check-before-copy is not an atomic destination lock. Stop `data_platform`
   before cutover.
3. Enable incoming versioning when keys can be overwritten. Exact-version
   deletion can expose an earlier version; the notification-driven mover does
   not scan that exposed version. For unversioned objects, ETag conditions cannot
   distinguish concurrent identical-content replacements; do not permit them.
4. Configure an asynchronous on-failure destination or DLQ and alarms for errors,
   throttling, dropped events, and failure-delivery errors. Set function-error
   retries to **0** if failures should go directly to manual reconciliation.
   AWS can still deliver duplicate events. A late DFM may require an operator
   redrive after it arrives; this is not indefinite polling.
5. Size the timeout for the largest multipart transfer (maximum 900 seconds).
   Use a runtime boto3 supporting conditional deletes; repository-pinned 1.40.45
   supports them. Configure lifecycle cleanup of incomplete multipart uploads.
6. Grant scoped S3 listing/reading, source version reads/deletes, destination
   writes and multipart aborts, and the relevant KMS permissions. Archive reads
   must cover relocated paths. Destination encryption follows bucket defaults.

Before production, exercise the real dev Lambda with a first LOAD, later chunks,
CDC, late/missing DFM, duplicates, partial failures, and a large multipart copy.
Verify bytes, snapshot paths, metadata removal, conditional/versioned deletion,
permissions, alarms, and the manual reconciliation process. Confirm throughput
under single concurrency and the configured timeout.

Drain and reconcile the old mover, populate the list, and ensure active tables
have incoming or retained archive snapshot state. Plan a separate replay for
preexisting incoming objects: adding notifications does not emit past creations.

## Local verification

```sh
python -m pytest tests/scripts/cubic_mover_lambda_test.py
ruff check scripts/cubic_mover_lambda.py tests/scripts/cubic_mover_lambda_test.py
mypy scripts/cubic_mover_lambda.py tests/scripts/cubic_mover_lambda_test.py
```

The tests use in-memory S3 operations plus real botocore request validation for
single and simulated 6 GiB multipart copies. They do not contact AWS or establish
that live infrastructure is configured correctly.
