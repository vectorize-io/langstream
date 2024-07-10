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

import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.code.SingleRecordAgentProcessor;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import ai.langstream.api.util.ConfigurationUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.utils.XMLReaderUtils;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

@Slf4j
public class TikaTextExtractorAgent extends SingleRecordAgentProcessor {

    private static final Document TIKA_CONFIG;

    private AutoDetectParser parser;
    private boolean fallbackToRaw;


    static {
        try {
            TIKA_CONFIG = XMLReaderUtils.buildDOM(new ByteArrayInputStream(
                    """
                            <properties>
                              <parsers>
                                <parser class="org.apache.tika.parser.DefaultParser">
                                  <parser-exclude class="org.apache.tika.parser.ocr.TesseractOCRParser"/>
                                </parser>
                              </parsers>
                            </properties>
                            """.getBytes(StandardCharsets.UTF_8)
            ));
        } catch (TikaException | IOException | SAXException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        // pass Tika configuration to disable OCR
        TikaConfig tikaConfig = new TikaConfig(TIKA_CONFIG);
        parser = new AutoDetectParser(tikaConfig);
        fallbackToRaw = ConfigurationUtils.getBoolean( "fallback-to-raw", false, configuration);
    }

    @Override
    public List<Record> processRecord(Record record) throws Exception {
        if (record == null) {
            return List.of();
        }

        Object value = record.value();
        final InputStream stream = Utils.toStream(value);
        Metadata metadata = new Metadata();
        StringWriter valueAsString = new StringWriter();

        log.info("Processing document, key {}", record.key());

        BodyContentHandler handler = new BodyContentHandler(valueAsString);
        long startTs = System.currentTimeMillis();
        String content;

        final Set<Header> newHeaders = new HashSet<>(record.headers());

        try {

            parser.parse(stream, handler, metadata);
            content = valueAsString.toString();
            String[] names = metadata.names();
            for (String name : names) {
                newHeaders.add(new SimpleRecord.SimpleHeader(name, String.join(", ", metadata.getValues(name))));
            }
        } catch (TikaException ex) {
            log.error("Error parsing document", ex);
            if (fallbackToRaw) {
                log.info("Falling back to plain text");
                content = String.valueOf(value);
                newHeaders.add(
                        new SimpleRecord.SimpleHeader("tika-parse-failed", "true"));
            } else {
                throw ex;
            }
        }
        newHeaders.add(
                new SimpleRecord.SimpleHeader("Content-Length", String.valueOf(content.length())));
        long time = (System.currentTimeMillis() - startTs) / 1000;
        log.info("Processed document in {} seconds, final size {} bytes, headers {}", time, content.length(), newHeaders);
        if (log.isDebugEnabled()) {
            log.debug("Content: {}", content);
        }

        SimpleRecord newRecord =
                SimpleRecord.copyFrom(record)
                        .value(content)
                        .headers(newHeaders)
                        .build();
        return List.of(newRecord);
    }
}
