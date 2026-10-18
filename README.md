# GCS-to-PostgreSQL Apache Beam Pipeline

A small Java example that reads a CSV object from a local Google Cloud Storage
(GCS) emulator, validates and parses each row, and writes valid users to
PostgreSQL with Apache Beam's `DirectRunner`.

The full local stack runs in Docker Compose. You do not need to install Java or
Maven on your computer to build, test, or run the pipeline; the pipeline image
uses Maven 3.9.16 and Eclipse Temurin Java 25 LTS.

## Contents

- [What the project does](#what-the-project-does)
- [Requirements](#requirements)
- [Run the complete example](#run-the-complete-example)
- [Run the tests](#run-the-tests)
- [Run the pipeline from your computer](#run-the-pipeline-from-your-computer)
- [Project layout](#project-layout)
- [Configuration reference](#configuration-reference)
- [How data is processed](#how-data-is-processed)
- [Troubleshooting](#troubleshooting)
- [Stop services and reset data](#stop-services-and-reset-data)
- [Limitations and security](#limitations-and-security)

## What the project does

```text
sample_users.csv
      | upload over HTTP
      v
fake-gcs-server (GCS-compatible local emulator)
      | gs://my-test-bucket/input_file.csv
      v
Apache Beam TextIO -> CsvParserFn -> UserRecord
      | JDBC prepared statement
      v
PostgreSQL users table
```

The pipeline is a **one-shot batch job**. It reads the configured object,
processes its lines, writes valid rows, and exits; it is not a continuously
running service.

## Requirements

- Docker Desktop with Docker Compose v2 (`docker compose`)
- PowerShell (commands below use PowerShell syntax)
- Network access the first time you build, so Docker can download the base
  image and Maven can download Java dependencies

For optional host-side development, install JDK 25 and Maven 3.9 or newer.
Docker is otherwise sufficient.

For Java editing in VS Code, open the project root (the folder containing
`pom.xml`) and install the recommended Java Extension Pack if prompted. The
Maven project import supplies the source roots and dependency classpath; do not
open `src/main/java` or `src/test/java` as separate workspaces.

## Run the complete example

Run every command from the project root (the directory containing
`docker-compose.yml`).

### 1. Start PostgreSQL and the GCS emulator

```powershell
docker compose up -d
docker compose ps
```

Wait until `postgres-db` reports `healthy` and `gcs-emulator` is running:

```powershell
docker compose ps
```

The first start creates the database and runs `init-scripts/init.sql` to create
the `users` table. PostgreSQL stores its data in the named `postgres-data`
volume, so subsequent starts keep existing rows.

### 2. Create the GCS bucket

Create the bucket once for the current emulator state:

```powershell
$bucketBody = @{ name = 'my-test-bucket' } | ConvertTo-Json -Compress
Invoke-RestMethod -Method Post `
  -Uri 'http://localhost:8080/storage/v1/b' `
  -ContentType 'application/json' `
  -Body $bucketBody
```

If the bucket already exists, do not create it again; continue to the upload.
The emulator's state is not persisted across container recreation.

### 3. Upload the sample CSV

```powershell
Invoke-WebRequest -Method Post `
  -Uri 'http://localhost:8080/upload/storage/v1/b/my-test-bucket/o?uploadType=media&name=input_file.csv' `
  -ContentType 'text/csv' `
  -InFile '.\sample_users.csv' `
  -UseBasicParsing
```

The local file `sample_users.csv` is uploaded as the GCS object
`gs://my-test-bucket/input_file.csv`. Repeat this upload whenever you edit the
sample file.

### 4. Build and run the pipeline container

```powershell
docker compose run --build --rm pipeline
```

This builds the image if needed and executes Maven's `exec:java` goal. The
pipeline container uses the Compose DNS names `gcs-emulator` and `postgres-db`
to connect to the other containers.

### 5. Verify the rows

```powershell
docker compose exec postgres-db psql -U postgres -d my_database `
  -c "SELECT id, name, email FROM users ORDER BY id;"
```

The sample file contains three users. Running the job again is safe for those
IDs: the database primary key and `ON CONFLICT (id) DO NOTHING` prevent
duplicate inserts. Existing rows are **not updated** if the CSV values change.

## Run the tests

The parser unit tests do not require the database, emulator, or uploaded file.
They run inside the same Java 25 Maven image:

```powershell
docker compose run --build --rm --entrypoint mvn pipeline -B -ntp test
```

The `--entrypoint mvn` option replaces the normal pipeline entry point so this
command runs Maven tests instead of starting the data pipeline.

## Run the pipeline from your computer

This is optional. Install JDK 25 and Maven 3.9 or newer, start the Compose
services and upload the CSV as described above, then run from the project root:

```powershell
mvn -B -ntp compile exec:java `
  "-Dexec.args=--inputFile=gs://my-test-bucket/input_file.csv --jdbcUrl=jdbc:postgresql://localhost:5432/my_database --dbUsername=postgres --dbPassword=yourextrasecretpassword --gcsEmulatorEndpoint=http://localhost:8080/storage/v1/"
```

When running from your computer, use `localhost` for both services. When the
pipeline runs in Compose, use `gcs-emulator` and `postgres-db` instead.

## Project layout

| Path | Purpose |
|---|---|
| `pom.xml` | Maven project, Java release, dependencies, compiler and run plugins |
| `Dockerfile` | Java 25/Maven image used to compile and run the pipeline |
| `docker-compose.yml` | Local GCS emulator, PostgreSQL, and one-shot pipeline services |
| `init-scripts/init.sql` | Creates the PostgreSQL `users` table on first database initialization |
| `sample_users.csv` | Small input dataset used by the walkthrough |
| `src/main/java/com/example/beam/GcsToPostgresPipeline.java` | Connects the Beam read, parse, map, and database-write stages |
| `src/main/java/com/example/beam/GcsToPostgresOptions.java` | Declares and validates command-line pipeline options |
| `src/main/java/com/example/beam/CsvParserFn.java` | Parses and filters individual CSV lines |
| `src/main/java/com/example/beam/UserRecord.java` | Serializable data model for a parsed user |
| `src/main/java/com/example/beam/DatabaseConfig.java` | Builds the Beam JDBC data-source configuration |
| `src/test/java/com/example/beam/CsvParserFnTest.java` | Unit tests for valid, malformed, and whitespace-padded CSV rows |
| `flow.md` | Beginner-friendly explanation of the data flow and concepts |
| `beam-pipeline-spec.md` | Detailed technical behavior and configuration reference |
| `.vscode/settings.json` | Maven import and Java null-analysis workspace settings |
| `.vscode/extensions.json` | Recommends the VS Code Java Extension Pack |
| `.dockerignore` | Excludes local/generated files from the Docker build context |

## Configuration reference

The Beam options are defined in `GcsToPostgresOptions.java` and passed as
`--name=value` command-line arguments:

| Option | Required | Default | Meaning |
|---|---:|---|---|
| `--inputFile` | Yes | — | GCS URI of the input object, for example `gs://my-test-bucket/input_file.csv` |
| `--jdbcUrl` | Yes | — | PostgreSQL JDBC URL |
| `--dbUsername` | Yes | — | PostgreSQL login |
| `--dbPassword` | Yes | — | PostgreSQL password |
| `--gcsEmulatorEndpoint` | No | `http://localhost:8080/storage/v1/` | GCS-compatible API endpoint; use `http://gcs-emulator:8080/storage/v1/` inside Compose |

The local Compose database uses `my_database`, username `postgres`, and
password `yourextrasecretpassword`. These are demo credentials only. Do not
reuse them for a deployed or publicly reachable database.

## How data is processed

1. Beam `TextIO` reads the GCS object one line at a time.
2. `CsvParserFn` ignores blank lines, the `id,name,email` header, malformed
   quoted values, and rows that do not contain exactly three fields. The ID
   must not be empty after trimming.
3. Each accepted row is mapped to a serializable `UserRecord`.
4. Beam `JdbcIO` binds each record's fields to a parameterized PostgreSQL
   `INSERT`. Parameter binding keeps row values separate from SQL syntax.
5. The primary key on `users.id` and `ON CONFLICT ... DO NOTHING` make repeated
   loads idempotent for IDs already present.

See [flow.md](./flow.md) for the fundamentals and
[beam-pipeline-spec.md](./beam-pipeline-spec.md) for implementation details.

## Troubleshooting

### PostgreSQL is not healthy

```powershell
docker compose ps
docker compose logs postgres-db
```

The pipeline service waits for PostgreSQL's health check. Inspect the logs if
initialization failed. Note that `init.sql` only runs when PostgreSQL creates a
new, empty data directory.

### GCS bucket or object not found

Check that the emulator is running and that the bucket was created and the
object uploaded:

```powershell
docker compose logs gcs-emulator
Invoke-RestMethod -Uri 'http://localhost:8080/storage/v1/b'
```

The bucket and uploaded object are stored by the emulator container, not the
PostgreSQL volume. If the emulator was recreated, create the bucket and upload
the file again.

### Maven or Java version mismatch

The Docker path always uses the versions declared by the `Dockerfile`. For a
host run, confirm `java -version` reports Java 25 and `mvn -version` uses that
JDK.

### VS Code shows unresolved Java imports or incorrect packages

Open the workspace folder that contains `pom.xml`, ensure the recommended Java
Extension Pack is installed, then run **Java: Clean Java Language Server
Workspace** from the Command Palette and reload the VS Code window. Wait for
the Maven project import to finish before relying on editor diagnostics.

### Inspect service logs

```powershell
docker compose logs --follow postgres-db
docker compose logs --follow gcs-emulator
```

Press `Ctrl+C` to stop following logs; this does not stop the containers.

## Stop services and reset data

Stop and remove containers and the Compose network while preserving PostgreSQL
data:

```powershell
docker compose down
```

To also permanently delete the PostgreSQL data volume, so the schema is
recreated on the next start:

```powershell
docker compose down -v
```

The `-v` option deletes saved database data. It does not delete files in the
project directory.

## Limitations and security

- This is a local learning/demo setup, not a production deployment.
- The GCS emulator runs without cloud authentication; the pipeline explicitly
  disables GCP credentials for this local endpoint.
- Demo database credentials are included in local Compose commands for
  convenience. Use secrets management and private networking for real systems.
- The CSV parser handles one physical line per record, quoted commas, escaped
  double quotes, and whitespace around values. It does not support quoted
  fields spanning multiple lines.
- Invalid rows are skipped rather than reported to a dead-letter output.
- Database conflicts are ignored; the pipeline does not update existing rows.
- `DirectRunner` executes locally and is intended for this example, not a
  distributed production workload.
