/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.nutch.crawl;

import org.apache.gora.mapreduce.GoraReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.GeneratorJob.*;
import org.apache.nutch.fetcher.FetcherJob.FetcherMapper;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nutch.crawl.GeneratorJob.*;

/** Reduce class for generate
 *
 * The #reduce() method write a random integer to all generated URLs. This random
 * number is then used by {@link FetcherMapper}.
 *
 */
public class GeneratorReducer
extends GoraReducer<SelectorEntry, WebPage, String, WebPage> {

  public static final Logger LOG = LoggerFactory.getLogger(GeneratorJob.class);

  protected long count = 0;
  private long limit;
  private long maxCount;
  private boolean byDomain = false;
  private Map<String, Integer> hostCountMap = new HashMap<String, Integer>();
  private String batchId;

  @Override
  protected void reduce(SelectorEntry key, Iterable<WebPage> values,
      Context context) throws IOException, InterruptedException {
    for (WebPage page : values) {
      if (limit!=0 && count >= limit) {
        GeneratorJob.LOG.trace("Skipping " + page.getKey()+ "; too many generated urls");
        context.getCounter("Generator", "LIMIT").increment(1);
        return;
      }
      if (maxCount > 0) {
        String hostordomain;
        if (byDomain) {
          hostordomain = URLUtil.getDomainName(key.getKey());
        } else {
          hostordomain = URLUtil.getHost(key.getKey());
        }

        Integer hostCount = hostCountMap.get(hostordomain);
        if (hostCount == null) {
          hostCountMap.put(hostordomain, 0);
          hostCount = 0;
        }
        if (hostCount >= maxCount) {
          context.getCounter("Generator", "HOST_LIMIT").increment(1);
          return;
        }
        hostCountMap.put(hostordomain, hostCount + 1);
      }

      Mark.GENERATE_MARK.putMark(page, batchId);
      page.setBatchId(batchId);
      try {
        context.write(TableUtil.reverseUrl(key.getKey()), page);
      } catch (MalformedURLException e) {
    	context.getCounter("Generator", "MALFORMED_URL").increment(1);
        continue;
      }
      context.getCounter("Generator", "GENERATE_MARK").increment(1);
      LOG.trace("GeneratedUrl : "+key.getKey());
      count++;
    }
  }

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    long totalLimit = conf.getLong(GeneratorJob.GENERATOR_TOP_N, 0);
    if (totalLimit == 0) {
      limit = 0;
    } else {
      limit = Math.max(totalLimit, 1);
      GeneratorJob.LOG.info("Limit:"+limit);
    }
    maxCount = conf.getLong(GENERATOR_MAX_COUNT, 0);
    GeneratorJob.LOG.info("Maxcount:"+maxCount);
    batchId = conf.get(BATCH_ID);
    String countMode = conf.get(GENERATOR_COUNT_MODE, GENERATOR_COUNT_VALUE_HOST);
    if (countMode.equals(GENERATOR_COUNT_VALUE_DOMAIN)) {
      byDomain = true;
    }

  }

}
