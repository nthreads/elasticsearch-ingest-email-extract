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

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLength;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class EmailExtractProcessorTests extends ESTestCase {

    public void testThatProcessWorksWithEmail() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "this is a test field pointing to nauman@csms.ae");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        //randomAsciiLettersOfLength
        //randomAsciiOfLength
        EmailExtractProcessor processor = new EmailExtractProcessor(randomAsciiLettersOfLength(10), "Description","source_field", "target_field");
        processor.execute(ingestDocument);
        Map<String, Object> data = ingestDocument.getSourceAndMetadata();
        assertThat(data, hasKey("target_field"));
        assertThat(data.get("target_field"), is(instanceOf(List.class)));
        @SuppressWarnings("unchecked")
        List<String> urls = (List<String>) data.get("target_field");
        assertThat(urls, containsInAnyOrder("nauman@csms.ae"));
    }
}

