/*
 * Copyright [2020] [Nauman Zubair]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.elasticsearch.plugin.ingest.email.extract;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.nibor.autolink.LinkExtractor;
import org.nibor.autolink.LinkType;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

public class EmailExtractProcessor extends AbstractProcessor {

    public static final String TYPE = "email_extract";

    private final String field;
    private final String targetField;

    public EmailExtractProcessor(String tag, String description, String field,
                 String targetField) throws IOException {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        String content = ingestDocument.getFieldValue(field, String.class);
        LinkExtractor linkExtractor = LinkExtractor.builder().linkTypes(EnumSet.of(LinkType.EMAIL)).build();
        List<String> links = StreamSupport.stream(linkExtractor.extractLinks(content).spliterator(), false)
                .map(link -> content.substring(link.getBeginIndex(), link.getEndIndex()))
                .collect(Collectors.toList());
        ingestDocument.setFieldValue(targetField, links);
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public EmailExtractProcessor create(Map<String, Processor.Factory> factories, String tag,
               String description, Map<String, Object> config) throws Exception {
            String field = readStringProperty(TYPE, tag, config, "field");
            String targetField = readStringProperty(TYPE, tag, config, "target_field", "default_field_name");

            return new EmailExtractProcessor(tag, description, field, targetField);
        }
    }
}
