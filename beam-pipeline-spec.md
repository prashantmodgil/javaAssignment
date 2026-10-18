# Technical specification: GCS to PostgreSQL Beam pipeline

This document describes the implementation and runtime contract. For a
step-by-step local run, see [README.md](./README.md). For the conceptual data
flow, see [flow.md](./flow.md).

## Purpose and scope

The application is a local batch-processing example. Apache Beam's
`DirectRunner` reads a CSV object through a GCS-compatible emulator, turns
acceptable rows into `UserRecord` values, and inserts them into PostgreSQL.
It is not a streaming service, production cloud deployment, or complete CSV
validation system.

## Runtime and build

| Component | Configuration |
|---|---|
| Java runtime/compiler target | Java 25 LTS (`maven.compiler.release`) |
| Container JDK distribution | Eclipse Temurin |
| Container Maven | 3.9.16 |
| Build tool | Maven, project descriptor `pom.xml` |
| Beam runner | `DirectRunner` |
| Local database | PostgreSQL 16 Alpine |
| Local GCS API | `fsouza/fake-gcs-server` (`latest` image tag in Compose) |

The Docker workflow is the consistent build environment and does not require a
host JDK or Maven. A host-side run requires JDK 25 and Maven 3.9 or newer.
Because the emulator image uses the floating `latest` tag, its contents may
change independently of this repository.

## Processing graph

```text
TextIO.read().from(inputFile)
  -> ParDo(CsvParserFn)
  -> SerializableCoder<String[]>
  -> MapElements<String[], UserRecord>
  -> SerializableCoder<UserRecord>
  -> JdbcIO.write()
```

### Source and parsing contract

- The input is a GCS URI, for example `gs://my-test-bucket/input_file.csv`.
- `TextIO` presents one physical line at a time to `CsvParserFn`.
- A row must have exactly three fields in the order `id,name,email`.
- Blank lines, the header, malformed quoting, other field counts, and empty
  IDs are skipped.
- Header detection is case-insensitive and tolerates a UTF-8 BOM before `id`.
- Quoted commas, escaped double quotes, and surrounding whitespace are
  supported. Quoted fields spanning multiple physical lines are not.
- Values are trimmed. The parser does not enforce non-empty name/email values
  or validate email syntax.
- Rejected rows are dropped; no error collection or dead-letter sink exists.

### Data model and coder

`UserRecord` is a top-level Java record with `id`, `name`, and `email`
components. It implements `Serializable`. The pipeline explicitly uses Beam's
`SerializableCoder` for both emitted `String[]` rows and mapped records.

### Database sink

The JDBC target statement is:

```sql
INSERT INTO users (id, name, email) VALUES (?, ?, ?)
ON CONFLICT (id) DO NOTHING
```

The values are assigned with a `PreparedStatementSetter`; CSV content is
provided as bound values and not concatenated into SQL. `id` is the table's
primary key. A duplicate ID is ignored, not updated.

## Command-line options

`GcsToPostgresOptions` extends Beam's `PipelineOptions`. The required options
are checked using Beam's validation before the pipeline is created.

| Name | Required | Default | Description |
|---|---:|---|---|
| `--inputFile` | Yes | None | GCS URI for the input CSV object |
| `--jdbcUrl` | Yes | None | JDBC URL for PostgreSQL |
| `--dbUsername` | Yes | None | PostgreSQL user |
| `--dbPassword` | Yes | None | PostgreSQL password |
| `--gcsEmulatorEndpoint` | No | `http://localhost:8080/storage/v1/` | GCS-compatible API endpoint |

The application casts its pipeline options to `GcsOptions`, configures the
endpoint, and sets GCP credentials to `null` before creating the pipeline.
This is specifically for the unauthenticated local emulator, not for a real
GCS service.

### Local addresses

When Java runs on the host, use `localhost` for both exposed container ports:

- GCS endpoint: `http://localhost:8080/storage/v1/`
- JDBC host: `localhost:5432`

When the pipeline itself runs as a Compose container, use service DNS:

- GCS endpoint: `http://gcs-emulator:8080/storage/v1/`
- JDBC host: `postgres-db:5432`

`localhost` inside the pipeline container would refer to that container itself.

## Database schema and initialization

`init-scripts/init.sql` creates the table:

```sql
CREATE TABLE IF NOT EXISTS users (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);
```

Compose mounts `init-scripts` into PostgreSQL's initialization directory.
PostgreSQL executes these scripts only when initializing an empty data
directory. The named volume `postgres-data` persists the database between
container runs, so changing `init.sql` does not automatically change an
already-initialized volume.

## Dependencies

The authoritative dependency versions are in `pom.xml`.

| Maven artifact | Version | Role |
|---|---:|---|
| `org.apache.beam:beam-runners-direct-java` | `${beam.version}` (`2.59.0`) | Local Beam runner |
| `org.apache.beam:beam-sdks-java-core` | `${beam.version}` (`2.59.0`) | Pipeline API, transforms, coders |
| `org.apache.beam:beam-sdks-java-io-google-cloud-platform` | `${beam.version}` (`2.59.0`) | GCS options and integration |
| `org.apache.beam:beam-sdks-java-io-jdbc` | `${beam.version}` (`2.59.0`) | JDBC sink |
| `org.postgresql:postgresql` | `42.7.2` | PostgreSQL JDBC driver |
| `junit:junit` | `4.13.2` (test scope) | Unit testing |

The compiler plugin uses `maven.compiler.release` from the project properties.
The exec plugin runs `com.example.beam.GcsToPostgresPipeline`.

## Local execution and tests

Use the Docker Compose procedure and PowerShell commands in
[README.md](./README.md) to start services, create the bucket, upload input,
run the pipeline, and query the results.

The parser tests can run independently of the database and emulator:

```powershell
docker compose run --build --rm --entrypoint mvn pipeline -B -ntp test
```

Build the application image without starting the job:

```powershell
docker compose build pipeline
```

Run the one-shot job after uploading the object:

```powershell
docker compose run --build --rm pipeline
```

The configured SQL makes reprocessing IDs safe from duplicate-key failures.
It does not update changed data or delete rows that were removed from the
source.

## Local infrastructure services

| Compose service | Purpose | Port on host | Persistence |
|---|---|---:|---|
| `gcs-emulator` | GCS-compatible storage API | `8080` | Emulator container state; recreate bucket/object when lost |
| `postgres-db` | Relational target database | `5432` | Named `postgres-data` volume |
| `pipeline` | Compiles/runs the one-shot job; only started on request | None | No persistent application data |

The pipeline is assigned the `pipeline` Compose profile, so the regular
`docker compose up -d` starts only the backing services. It waits for the
PostgreSQL health check and for the emulator container to start before running.
The emulator dependency is a container-start condition, not an application
health check.

The database password in Compose is a local demo value. Never use it for a
public or production database.
