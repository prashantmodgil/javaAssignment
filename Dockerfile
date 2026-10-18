# Use the same Java 25 toolchain for compilation and execution.
FROM maven:3.9.16-eclipse-temurin-25

WORKDIR /app

# Resolve dependencies before copying source so Docker can cache this layer
# when only application code changes.
COPY pom.xml .
RUN mvn -B -ntp dependency:go-offline

COPY src ./src
RUN mvn -B -ntp -DskipTests compile

# Compose supplies the endpoint and connection options as exec.args.
ENTRYPOINT ["mvn", "-B", "-ntp", "exec:java", "-Dexec.mainClass=com.example.beam.GcsToPostgresPipeline"]
CMD ["-Dexec.args=--inputFile=gs://my-test-bucket/input_file.csv --jdbcUrl=jdbc:postgresql://postgres-db:5432/my_database --dbUsername=postgres --dbPassword=yourextrasecretpassword --gcsEmulatorEndpoint=http://gcs-emulator:8080/storage/v1/"]
