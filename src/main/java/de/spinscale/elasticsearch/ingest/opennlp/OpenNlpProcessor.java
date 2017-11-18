/*
 * Copyright [2016] [Alexander Reelsen]
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

/* bsn 8/16/2017 add ignoreMissing and DOCCAT processing */

package de.spinscale.elasticsearch.ingest.opennlp;

import org.elasticsearch.common.Strings;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalList;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty; /*bsn*/

/*bsn*/
import org.apache.logging.log4j.Logger; 
import org.apache.logging.log4j.message.ParameterizedMessage; 
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.logging.Loggers;
/*bsn*/

/*bsn*/
/*import org.apache.commons.lang; StringUtils*/
import java.util.regex.Pattern;
import java.util.regex.Matcher;
/*bsn*/

public class OpenNlpProcessor extends AbstractProcessor {

    public static final String TYPE = "opennlp";

    private final OpenNlpService openNlpService;
    private final String sourceField;
    private final String targetField;
    private final Set<String> fields;
    private final Logger logger; /*bsn*/

    private final boolean ignoreMissing; /*bsn*/

    /*bsn*/
    public boolean containsIgnoreCase( String haystack, String needle ) {
      if(needle.equals(""))
        return true;
      if(haystack == null || needle == null || haystack .equals(""))
        return false;

      Pattern p = Pattern.compile(needle,Pattern.CASE_INSENSITIVE+Pattern.LITERAL);
      Matcher m = p.matcher(haystack);
      return m.find();
    }
    /*bsn*/

    OpenNlpProcessor(OpenNlpService openNlpService, String tag, String sourceField, 
       String targetField, Set<String> fields, boolean ignoreMissing) 
       throws IOException { /*bsn*/
        super(tag);
        this.openNlpService = openNlpService;
        this.sourceField = sourceField;
        this.targetField = targetField;
        this.fields = fields;
        this.ignoreMissing = ignoreMissing; /*bsn*/
        this.logger = Loggers.getLogger(getClass()); /*bsn*/
    }

    boolean isIgnoreMissing() {  /*bsn*/
        return ignoreMissing;    /*bsn*/
    }                            /*bsn*/

    @Override
    public void execute(IngestDocument ingestDocument) throws Exception {
        String content = ingestDocument.getFieldValue(sourceField, String.class, ignoreMissing); /*bsn*/

        /*bsn*/
        if (content == null && ignoreMissing) {
            return;
        } else if (content == null) {
            throw new IllegalArgumentException("field [" + sourceField + "] is null, cannot extract .");
        }
        /*bsn*/

        if (Strings.hasLength(content)) {
            Map<String, Set<String>> entities = new HashMap<>();
            mergeExisting(entities, ingestDocument, targetField);

            for (String field : fields) {
                if( !containsIgnoreCase( field,"category") ) {
                  Set<String> data = openNlpService.find(content, field);
                  merge(entities, field, data);
                  /*logger.info("ner entities in [{}] ", entities );
                  logger.info("ner field in [{}] ", field );
                  logger.info("ner data in [{}] ", data );*/
                }
            }

            ingestDocument.setFieldValue(targetField, entities);
        }

        /*bsn*/
        if (Strings.hasLength(content)) {
            Map<String, Set<String>> entities = new HashMap<>();
            mergeExisting(entities, ingestDocument, targetField);

            for (String field : fields) {
                if( containsIgnoreCase( field, "category") ) {
                  Set<String> data = openNlpService.categorize(content, field);
                  merge(entities, field, data);
                  /*logger.info("doccat entities in [{}] ", entities );
                  logger.info("doccat field in [{}] ", field );
                  logger.info("doccat data in [{}] ", data );*/
                }
            }

            ingestDocument.setFieldValue(targetField, entities);
        }
        /*bsn*/
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        private OpenNlpService openNlpService;

        public Factory(OpenNlpService openNlpService) {
            this.openNlpService = openNlpService;
        }

        @Override
        public OpenNlpProcessor create(Map<String, Processor.Factory> registry, String processorTag, Map<String, Object> config)
                throws Exception {
            String field = readStringProperty(TYPE, processorTag, config, "field");
            String targetField = readStringProperty(TYPE, processorTag, config, "target_field", "entities");
            List<String> fields = readOptionalList(TYPE, processorTag, config, "fields");
            final Set<String> foundFields = fields == null || fields.size() == 0 ? openNlpService.getModels() : new HashSet<>(fields);
            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false); /*bsn*/

            return new OpenNlpProcessor(openNlpService, processorTag, field, targetField, foundFields, ignoreMissing); /*bsn*/
        }
    }

    private static void mergeExisting(Map<String, Set<String>> entities, IngestDocument ingestDocument, String targetField) {
        if (ingestDocument.hasField(targetField)) {
            @SuppressWarnings("unchecked")
            Map<String, Set<String>> existing = ingestDocument.getFieldValue(targetField, Map.class);
            entities.putAll(existing);
        } else {
            ingestDocument.setFieldValue(targetField, entities);
        }
    }

    private static void merge(Map<String, Set<String>> map, String key, Set<String> values) {
        if (values.size() == 0) return;

        if (map.containsKey(key))
            values.addAll(map.get(key));

        map.put(key, values);
    }
}
