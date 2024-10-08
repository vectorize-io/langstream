/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.agents.text;

import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.code.SingleRecordAgentProcessor;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.tika.exception.TikaException;
import org.junit.jupiter.api.Test;

@Slf4j
public class TextExtractorTest {

    @Test
    public void textExtractFromText() throws Exception {
        try (SingleRecordAgentProcessor instance = createProcessor(); ) {

            Record fromSource =
                    SimpleRecord.builder()
                            .key("filename.txt")
                            .value("This is a test".getBytes(StandardCharsets.UTF_8))
                            .origin("origin")
                            .timestamp(System.currentTimeMillis())
                            .build();

            Record result = instance.processRecord(fromSource).get(0);
            log.info("Result: {}", result);
            assertEquals("This is a test", result.value().toString().trim());
            Collection<Header> headers = result.headers();
            assertTrue(
                    headers.stream()
                            .anyMatch(h -> h.key().equals("Content-Type") && h.value() != null),
                    "Header 'Content-Type' not found");
            assertTrue(
                    headers.stream()
                            .anyMatch(h -> h.key().equals("Content-Length") && h.value() != null),
                    "Header 'Content-Length' not found");

            assertTrue(
                    headers.stream()
                            .anyMatch(
                                    h ->
                                            h.key().equals("Content-Type")
                                                    && h.value()
                                                            .equals(
                                                                    "text/plain; charset=ISO-8859-1")),
                    "Header 'Content-Type' value is not 'text/plain; charset=ISO-8859-1'");

            assertTrue(
                    headers.stream()
                            .anyMatch(
                                    h ->
                                            h.key().equals("Content-Length")
                                                    && h.value().equals("15")),
                    "Header 'Content-Length' value is not '15'");
        }
    }

    @Test
    public void textExtractFromPdf() throws Exception {
        try (SingleRecordAgentProcessor instance = createProcessor(); ) {

            byte[] content = Files.readAllBytes(Paths.get("src/test/resources/simple.pdf"));

            Record fromSource =
                    SimpleRecord.builder()
                            .key("filename.pdf")
                            .value(content)
                            .origin("origin")
                            .timestamp(System.currentTimeMillis())
                            .build();

            Record result = instance.processRecord(fromSource).get(0);
            log.info("Result: {}", result);

            assertEquals("This is a very simple PDF", result.value().toString().trim());

            Collection<Header> headers = result.headers();
            assertTrue(
                    headers.stream()
                            .anyMatch(h -> h.key().equals("Content-Type") && h.value() != null),
                    "Header 'Content-Type' not found");
            assertTrue(
                    headers.stream()
                            .anyMatch(h -> h.key().equals("Content-Length") && h.value() != null),
                    "Header 'Content-Length' not found");

            // Assert that the Content-Type header value is text/plain
            assertTrue(
                    headers.stream()
                            .anyMatch(
                                    h ->
                                            h.key().equals("Content-Type")
                                                    && h.value().equals("application/pdf")),
                    "Header 'Content-Type' value is not 'application/pdf'");

            assertTrue(
                    headers.stream()
                            .anyMatch(
                                    h ->
                                            h.key().equals("Content-Length")
                                                    && h.value().equals("29")),
                    "Header 'Content-Length' value is not '29'");
        }
    }

    @SneakyThrows
    private static SingleRecordAgentProcessor createProcessor() {
        return createProcessor(Map.of());
    }

    @SneakyThrows
    private static SingleRecordAgentProcessor createProcessor(Map<String, Object> map) {
        TextProcessingAgentsCodeProvider provider = new TextProcessingAgentsCodeProvider();
        SingleRecordAgentProcessor instance = provider.createInstance("text-extractor");
        instance.init(map);
        return instance;
    }

    @Test
    public void textExtractFromWord() throws Exception {
        try (SingleRecordAgentProcessor instance = createProcessor(); ) {

            byte[] content = Files.readAllBytes(Paths.get("src/test/resources/simple.docx"));

            Record fromSource =
                    SimpleRecord.builder()
                            .key("filename.doc")
                            .value(content)
                            .origin("origin")
                            .timestamp(System.currentTimeMillis())
                            .build();

            Record result = instance.processRecord(fromSource).get(0);
            log.info("Result: {}", result);

            assertEquals("This is a very simple Word Document", result.value().toString().trim());

            Collection<Header> headers = result.headers();
            assertTrue(
                    headers.stream()
                            .anyMatch(h -> h.key().equals("Content-Type") && h.value() != null),
                    "Header 'Content-Type' not found");
            assertTrue(
                    headers.stream()
                            .anyMatch(h -> h.key().equals("Content-Length") && h.value() != null),
                    "Header 'Content-Length' not found");
            assertTrue(
                    headers.stream()
                            .anyMatch(
                                    h ->
                                            h.key().equals("Content-Type")
                                                    && h.value()
                                                            .equals(
                                                                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document")),
                    "Header 'Content-Type' value is not 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'");

            assertTrue(
                    headers.stream()
                            .anyMatch(
                                    h ->
                                            h.key().equals("Content-Length")
                                                    && h.value().equals("38")),
                    "Header 'Content-Length' value is not '38'");
        }
    }

    @Test
    public void testFallbackToTxt() throws Exception {
        byte[] content = Files.readAllBytes(Paths.get("src/test/resources/invalid.csv"));
        Record fromSource =
                SimpleRecord.builder()
                        .key("filename.csv")
                        .value(content)
                        .origin("origin")
                        .timestamp(System.currentTimeMillis())
                        .build();
        try (SingleRecordAgentProcessor instance = createProcessor(); ) {
            assertThrows(TikaException.class, () -> instance.processRecord(fromSource));
        }
        try (SingleRecordAgentProcessor instance =
                createProcessor(Map.of("fallback-to-raw", "true")); ) {
            Record record = instance.processRecord(fromSource).get(0);
            assertFalse(((String) record.value()).isBlank());
            assertEquals("true", record.getHeader("tika-parse-failed").valueAsString());
        }
    }
}
