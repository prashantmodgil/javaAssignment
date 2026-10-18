package com.example.beam;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/** Command-line inputs for the pipeline; connection values are required. */
public interface GcsToPostgresOptions extends PipelineOptions {
    @Description("GCS URI of the input CSV file")
    @Validation.Required
    String getInputFile();

    void setInputFile(String inputFile);

    @Description("JDBC URL of the PostgreSQL database")
    @Validation.Required
    String getJdbcUrl();

    void setJdbcUrl(String jdbcUrl);

    @Description("PostgreSQL username")
    @Validation.Required
    String getDbUsername();

    void setDbUsername(String dbUsername);

    @Description("PostgreSQL password")
    @Validation.Required
    String getDbPassword();

    void setDbPassword(String dbPassword);

    @Description("GCS emulator storage API endpoint (use the Compose service name in Docker)")
    @Default.String("http://localhost:8080/storage/v1/")
    String getGcsEmulatorEndpoint();

    void setGcsEmulatorEndpoint(String gcsEmulatorEndpoint);
}
