package com.example.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;

public class CsvParserFnTest {
    @Test
    public void parsesValidRowsAndSkipsHeadersAndMalformedRows() {
        Pipeline pipeline = Pipeline.create();

        PAssert.that(pipeline
                .apply(Create.of(
                        "id,name,email",
                        "u1,Alice,alice@example.com",
                        "u2,\"Smith, Bob\",bob@example.com",
                        "u3,\"Said \"\"hello\"\"\",carol@example.com",
                        "",
                        "u4,Missing email",
                        ",No ID,missing@example.com",
                        "u5,Extra,columns,here",
                        "u6,\"Unclosed quote,email@example.com"))
                .apply(ParDo.of(new CsvParserFn()))
                .apply(MapElements.into(TypeDescriptors.strings())
                        .via(fields -> String.join("|", fields))))
                .containsInAnyOrder(
                        "u1|Alice|alice@example.com",
                        "u2|Smith, Bob|bob@example.com",
                        "u3|Said \"hello\"|carol@example.com");

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void trimsWhitespaceAroundCsvValues() {
        Pipeline pipeline = Pipeline.create();

        PAssert.that(pipeline
                .apply(Create.of(" u7 , Dana , dana@example.com "))
                .apply(ParDo.of(new CsvParserFn()))
                .apply(MapElements.into(TypeDescriptors.strings())
                        .via(fields -> String.join("|", fields))))
                .containsInAnyOrder("u7|Dana|dana@example.com");

        pipeline.run().waitUntilFinish();
    }
}
