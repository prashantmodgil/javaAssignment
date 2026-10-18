# How the pipeline works

This guide explains the example's data flow and the main Apache Beam, GCS, and
database concepts. To run the full example, follow [README.md](./README.md).
The implementation details and option reference are in
[beam-pipeline-spec.md](./beam-pipeline-spec.md).

## The data journey

```text
sample_users.csv on your computer
     |
     | HTTP upload (one-time setup; repeat after editing the CSV)
     v
fake-gcs-server
     |
     | gs://my-test-bucket/input_file.csv
     v
Beam TextIO reads one physical line at a time
     |
     v
CsvParserFn filters invalid lines and emits String[] rows
     |
     v
MapElements turns each row into a UserRecord
     |
     v
JdbcIO binds the fields and executes a PostgreSQL INSERT
     |
     v
users table in PostgreSQL
```

## Concepts in this example

### Google Cloud Storage object and emulator

GCS stores data as **objects** inside **buckets**. A URI such as
`gs://my-test-bucket/input_file.csv` identifies an object, not a local file
path. The project uses `fsouza/fake-gcs-server` as a local GCS-compatible API,
so this example does not need a Google Cloud account or cloud bucket.

The emulator runs in Docker. Your computer reaches it at
`http://localhost:8080`; containers on the Compose network reach it by the
service name `gcs-emulator`. The bucket and object must be created/uploaded
before running the pipeline.

### Apache Beam pipeline and runner

Apache Beam describes data processing as a pipeline graph. In this project:

- A **Pipeline** is the graph built by `GcsToPostgresPipeline`.
- A **PCollection** is a collection of values flowing between graph steps.
- `TextIO` is the source that turns the object's text into lines.
- `ParDo` applies `CsvParserFn` independently to each line. The function may
  emit a row or emit nothing when the input is not accepted.
- `MapElements` converts the parser's three strings into a `UserRecord`.
- `JdbcIO` is the sink that writes records using JDBC.
- The **DirectRunner** executes the graph locally. It is simple for a
  demonstration but is not a distributed production runner.

Building the graph does not process data by itself. `pipeline.run()` starts
execution, and `waitUntilFinish()` keeps this one-shot Java process alive until
the job completes or fails.

### CSV parsing rules

`CsvParserFn` is intentionally small and scoped to the example's line-based
CSV:

- Blank lines and the header row are skipped. A UTF-8 byte-order mark before
  the header's `id` is tolerated.
- Accepted rows must contain exactly three fields: ID, name, and email.
- A missing ID and malformed quoting are rejected.
- Quoted commas and escaped double quotes (`""`) inside quoted values are
  supported.
- Whitespace surrounding each resulting field is trimmed.
- Quotes must begin at the start of a field. After a closing quote, only
  whitespace or a comma is accepted.
- A quoted field cannot span multiple physical lines.

The parser does not check whether name or email is non-empty, nor does it
validate that email values have a particular format. Invalid rows are silently
skipped by this demo; there is no dead-letter output.

### Java record and serialization

`UserRecord` names the three values instead of passing around anonymous array
indexes throughout the rest of the pipeline. It implements `Serializable`
because Beam may need to encode values as they move between transforms. The
pipeline explicitly assigns Beam `SerializableCoder`s to the array and record
collections.

### JDBC and repeat runs

`DatabaseConfig` supplies the PostgreSQL JDBC driver and connection details.
`JdbcIO` uses a prepared statement with placeholders (`?`) and binds ID, name,
and email as values. This keeps row contents separate from SQL syntax.

`users.id` is the primary key. The insert statement uses
`ON CONFLICT (id) DO NOTHING`; a repeated ID is ignored rather than inserted
again or updated. This makes loading the same sample IDs repeatable, but it is
not a general-purpose update or deduplication strategy for other data.

## Where to look in the code

| File | Responsibility |
|---|---|
| `GcsToPostgresPipeline.java` | Constructs and runs the Beam graph |
| `GcsToPostgresOptions.java` | Declares command-line inputs and validation |
| `CsvParserFn.java` | Parses and filters CSV lines |
| `UserRecord.java` | Serializable domain record |
| `DatabaseConfig.java` | Creates the Beam JDBC data-source configuration |
| `CsvParserFnTest.java` | Tests accepted and rejected parser examples |
| `init-scripts/init.sql` | Creates the PostgreSQL table on initial database setup |
| `docker-compose.yml` | Runs the emulator, database, and pipeline container |
| `Dockerfile` | Provides Java 25 and Maven for compile/run |

## Network names depend on where Java runs

Inside Docker Compose, `localhost` would refer to the pipeline container itself,
not another service. Therefore the container uses:

| Service | Pipeline container address | Address from your computer |
|---|---|---|
| GCS emulator | `http://gcs-emulator:8080` | `http://localhost:8080` |
| PostgreSQL | `postgres-db:5432` | `localhost:5432` |

The `--gcsEmulatorEndpoint` option selects the emulator API endpoint. The
PostgreSQL hostname is part of the `--jdbcUrl`.

## Lifecycle at a glance

1. Docker Compose starts PostgreSQL and the GCS emulator.
2. PostgreSQL creates its database and table on the first start with a fresh
   data volume.
3. You create a bucket and upload the CSV through the emulator's HTTP API.
4. Compose runs the pipeline container as a one-shot job.
5. The pipeline exits after processing. PostgreSQL continues running and keeps
   its rows in the named volume.
6. `docker compose down` removes containers but preserves database data.
   `docker compose down -v` also deletes the database volume.
