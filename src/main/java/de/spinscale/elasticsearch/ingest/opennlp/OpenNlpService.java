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

/* bsn 8/16/2017 add ignoreMissing DOCCAT processing*/

package de.spinscale.elasticsearch.ingest.opennlp;


/*bsn*/
import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.doccat.DocumentCategorizerME;
/*bsn*/


import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.util.Span;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/*bsn*/
/*import org.apache.commons.lang; StringUtils*/
import java.util.regex.Pattern;
import java.util.regex.Matcher;
/*bsn*/


/**
 * OpenNLP name finders are not thread safe, so we load them via a thread local hack
 */
public class OpenNlpService {

    private final Path configDirectory;
    private final Logger logger;
    private Settings settings;

    private ThreadLocal<TokenNameFinderModel> threadLocal = new ThreadLocal<>();
    private Map<String, TokenNameFinderModel> nameFinderModels = new ConcurrentHashMap<>();

    /*bsn*/
    private ThreadLocal<DoccatModel> doccatThreadLocal = new ThreadLocal<>(); 
    private Map<String, DoccatModel> doccatModels = new ConcurrentHashMap<>(); 
    /*bsn*/

    public OpenNlpService(Path configDirectory, Settings settings) {
        this.logger = Loggers.getLogger(getClass(), settings);
        this.configDirectory = configDirectory;
        this.settings = settings;
    }

    public Set<String> getModels() {
        return IngestOpenNlpPlugin.MODEL_FILE_SETTINGS.get(settings).getAsMap().keySet();
    }

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

    protected OpenNlpService start() {
        StopWatch sw = new StopWatch("models-loading");

        Map<String, String> settingsMap = IngestOpenNlpPlugin.MODEL_FILE_SETTINGS.get(settings).getAsMap();

        for (Map.Entry<String, String> entry : settingsMap.entrySet()) {
            String name = entry.getKey();
            sw.start(name);

            /*logger.info("model name [{}] ", name);*/

            if( !containsIgnoreCase( name, "category") ) { 
                logger.info("ner model name [{}] ", name);
                Path path = configDirectory.resolve(entry.getValue());
                try (InputStream is = Files.newInputStream(path)) {
                    nameFinderModels.put(name, new TokenNameFinderModel(is));
                } catch (IOException e) {
                    logger.error((Supplier<?>) () -> new ParameterizedMessage("Could not load model [{}] with path [{}]", name, path), e);
                }
            }

            /*bsn*/
            if( containsIgnoreCase( name, "category") ) { 
                logger.info("doccat model name [{}] ", name);
                Path path = configDirectory.resolve(entry.getValue());
                try (InputStream is = Files.newInputStream(path)) {
                    doccatModels.put(name, new DoccatModel(is));
                } catch (IOException e) {
                    logger.error((Supplier<?>) () -> new ParameterizedMessage("Could not load model [{}] with path [{}]", name, path), e);
                }
            }
            /*bsn*/

            sw.stop();
        }

        if (settingsMap.keySet().size() == 0) {
            logger.error("Did not load any models for ingest-opennlp plugin, none configured");
        } else {
            logger.info("Read models in [{}] for {}", sw.totalTime(), settingsMap.keySet());
        }

        return this;
    }

    public Set<String> find(String content, String field) {
        try {
            if (!nameFinderModels.containsKey(field)) {
                throw new ElasticsearchException("Could not find field [{}], possible values {}", field, nameFinderModels.keySet());
            }
            TokenNameFinderModel finderModel= nameFinderModels.get(field);
            if (threadLocal.get() == null || !threadLocal.get().equals(finderModel)) {
                threadLocal.set(finderModel);
            }

            String[] tokens = SimpleTokenizer.INSTANCE.tokenize(content);
            Span spans[] = new NameFinderME(finderModel).find(tokens);
            String[] names = Span.spansToStrings(spans, tokens);
            return Sets.newHashSet(names);
        } finally {
            threadLocal.remove();
        }
    }

    /*bsn*/
    public Set<String> categorize(String content, String field) {
        try {
            if (!doccatModels.containsKey(field)) {
                throw new ElasticsearchException("Could not find field [{}], possible values {}", field, doccatModels.keySet());
            }
            DoccatModel catModel = doccatModels.get(field);
            if (doccatThreadLocal.get() == null || !doccatThreadLocal.get().equals(catModel)) {
                doccatThreadLocal.set(catModel);
            }

            String[] tokens = SimpleTokenizer.INSTANCE.tokenize(content);
            DocumentCategorizerME categorizer = new DocumentCategorizerME(catModel);
            double[] distribution = categorizer.categorize(tokens);
            String category = categorizer.getBestCategory(distribution);
            return Sets.newHashSet(category);
        } finally {
            doccatThreadLocal.remove();
        }
    }
    /*bsn*/

}
