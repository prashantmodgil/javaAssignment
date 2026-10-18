package com.example.beam;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Builds and runs the one-shot GCS-to-PostgreSQL Beam pipeline. */
public class GcsToPostgresPipeline {
    public static void main(String[] args) {
        GcsToPostgresOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(GcsToPostgresOptions.class);

        // Redirect Beam's GCS client to the local emulator instead of real GCS.
        GcsOptions gcsOptions = options.as(GcsOptions.class);
        gcsOptions.setGcsEndpoint(options.getGcsEmulatorEndpoint());
        gcsOptions.setGcpCredential(null);
        options.setRunner(DirectRunner.class);

        DatabaseConfig databaseConfig = DatabaseConfig.from(options);
        Pipeline pipeline = Pipeline.create(options);

        // TextIO emits lines; the parser emits only acceptable three-field rows.
        PCollection<String[]> rows = pipeline
                .apply("Read CSV from GCS", TextIO.read().from(options.getInputFile()))
                .apply("Parse CSV rows", ParDo.of(new CsvParserFn()))
                .setCoder(SerializableCoder.of(String[].class));

        // Convert positional CSV fields to a named record before writing.
        PCollection<UserRecord> users = rows
                .apply("Map rows to users", MapElements
                        .into(TypeDescriptor.of(UserRecord.class))
                        .via((String[] fields) ->
                                new UserRecord(fields[0], fields[1], fields[2])))
                .setCoder(SerializableCoder.of(UserRecord.class));

        users.apply("Write users to PostgreSQL", JdbcIO.<UserRecord>write()
                .withDataSourceConfiguration(databaseConfig.toDataSourceConfiguration())
                // Existing IDs are intentionally ignored; this is not an upsert.
                .withStatement("INSERT INTO users (id, name, email) VALUES (?, ?, ?) "
                        + "ON CONFLICT (id) DO NOTHING")
                .withPreparedStatementSetter((user, statement) -> {
                    statement.setString(1, user.id());
                    statement.setString(2, user.name());
                    statement.setString(3, user.email());
                }));

        pipeline.run().waitUntilFinish();
    }
}
