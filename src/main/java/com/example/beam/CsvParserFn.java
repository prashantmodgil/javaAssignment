package com.example.beam;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.beam.sdk.transforms.DoFn;

/** Parses a single physical CSV line and emits only valid three-field rows. */
public class CsvParserFn extends DoFn<String, String[]> {
    @ProcessElement
    public void processElement(ProcessContext context) {
        String line = context.element();
        if (line == null || line.isBlank()) {
            return;
        }

        List<String> fields = parseCsvLine(line);
        if (fields == null || isHeader(fields) || fields.size() != 3) {
            return;
        }

        String[] row = fields.stream()
                .map(field -> Objects.requireNonNull(field).trim())
                .toArray(String[]::new);
        if (row[0].isEmpty()) {
            return;
        }

        context.output(row);
    }

    private static boolean isHeader(List<String> fields) {
        return fields.size() == 3
                && fields.get(0).replace("\uFEFF", "").trim().equalsIgnoreCase("id")
                && fields.get(1).trim().equalsIgnoreCase("name")
                && fields.get(2).trim().equalsIgnoreCase("email");
    }

    private static List<String> parseCsvLine(String line) {
        List<String> fields = new ArrayList<>(3);
        StringBuilder field = new StringBuilder();
        boolean inQuotes = false;
        // Distinguish a closing quote from a quote embedded in an unquoted field.
        boolean quoteClosed = false;

        for (int i = 0; i < line.length(); i++) {
            char current = line.charAt(i);

            if (inQuotes) {
                if (current == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        field.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                        quoteClosed = true;
                    }
                } else {
                    field.append(current);
                }
                continue;
            }

            if (quoteClosed) {
                if (current == ',') {
                    fields.add(field.toString());
                    field.setLength(0);
                    quoteClosed = false;
                } else if (!Character.isWhitespace(current)) {
                    return null;
                }
                continue;
            }

            if (current == '"') {
                if (field.length() != 0) {
                    return null;
                }
                inQuotes = true;
            } else if (current == ',') {
                fields.add(field.toString());
                field.setLength(0);
            } else {
                field.append(current);
            }
        }

        if (inQuotes) {
            return null;
        }

        fields.add(field.toString());
        return fields;
    }
}
