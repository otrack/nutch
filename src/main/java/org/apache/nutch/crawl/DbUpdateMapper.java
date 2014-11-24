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

import org.apache.gora.filter.FilterOp;
import org.apache.gora.filter.SingleFieldValueFilter;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Link;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DbUpdateMapper
  extends GoraMapper<String, WebPage, String, WebPage> {

  public static final Logger LOG = DbUpdaterJob.LOG;

  private int retryMax;

  private int maxInterval;
  private FetchSchedule schedule;
  private ScoringFilters scoringFilters;
  private List<ScoreDatum> inlinkedScoreData = new ArrayList<>();
  private int maxLinks;
  private DataStore<String,Link> linkDB;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    retryMax = conf.getInt("db.fetch.retry.max", 3);
    maxInterval = conf.getInt("db.fetch.interval.max", 0 );
    schedule = FetchScheduleFactory.getFetchSchedule(conf);
    scoringFilters = new ScoringFilters(conf);
    maxLinks = conf.getInt("db.update.max.inlinks", 10000);
    try {
      linkDB = StorageUtils.createStore(
        context.getConfiguration(), String.class, Link.class);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

  }

  @Override
  protected void map(String key, WebPage page, Context context) throws IOException, InterruptedException {

    String url;
    try {
      url = TableUtil.unreverseUrl(key);
    } catch (Exception e) {
      // this can happen because a newly discovered malformed link
      // may slip by url filters
      // TODO: Find a better solution
      return;
    }

    // compute in links

    // define filter
    SingleFieldValueFilter<String,Link> filter = new SingleFieldValueFilter<>();
    filter.setFieldName("out");
    filter.setFilterOp(FilterOp.EQUALS);
    List operand = new ArrayList<>();
    operand.add(key);
    filter.setOperands(operand);

    // build query
    inlinkedScoreData.clear();
    Query<String,Link> q = linkDB.newQuery();
    q.setFilter(filter);
    Result<String,Link> result = q.execute();

    // update scoreData
    try {
      while(result.next()) {
        Link link  = result.get();
        inlinkedScoreData.add(
          new ScoreDatum(link.getScore(), link.getIn(), "",link.getDistance()));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    byte status = page.getStatus().byteValue();
    switch (status) {
      case CrawlStatus.STATUS_FETCHED:         // succesful fetch
      case CrawlStatus.STATUS_REDIR_TEMP:      // successful fetch, redirected
      case CrawlStatus.STATUS_REDIR_PERM:
      case CrawlStatus.STATUS_NOTMODIFIED:     // successful fetch, notmodified
        int modified = FetchSchedule.STATUS_UNKNOWN;
        if (status == CrawlStatus.STATUS_NOTMODIFIED) {
          modified = FetchSchedule.STATUS_NOTMODIFIED;
        }
        ByteBuffer prevSig = page.getPrevSignature();
        ByteBuffer signature = page.getSignature();
        if (prevSig != null && signature != null) {
          if (SignatureComparator.compare(prevSig, signature) != 0) {
            modified = FetchSchedule.STATUS_MODIFIED;
          } else {
            modified = FetchSchedule.STATUS_NOTMODIFIED;
          }
        }
        long fetchTime = page.getFetchTime();
        long prevFetchTime = page.getPrevFetchTime();
        long modifiedTime = page.getModifiedTime();
        long prevModifiedTime = page.getPrevModifiedTime();
        String lastModified = page.getHeaders().get("Last-Modified");
        if ( lastModified != null ){
          try {
            modifiedTime = HttpDateFormat.toLong(lastModified.toString());
            prevModifiedTime = page.getModifiedTime();
          } catch (Exception e) {
          }
        }
        schedule.setFetchSchedule(url, page, prevFetchTime, prevModifiedTime,
          fetchTime, modifiedTime, modified);
        if (maxInterval < page.getFetchInterval())
          schedule.forceRefetch(url, page, false);
        break;
      case CrawlStatus.STATUS_RETRY:
        schedule.setPageRetrySchedule(url, page, 0L, page.getPrevModifiedTime(), page.getFetchTime());
        if (page.getRetriesSinceFetch() < retryMax) {
          page.setStatus((int)CrawlStatus.STATUS_UNFETCHED);
        } else {
          page.setStatus((int)CrawlStatus.STATUS_GONE);
        }
        break;
      case CrawlStatus.STATUS_GONE:
        schedule.setPageGoneSchedule(url, page, 0L, page.getPrevModifiedTime(), page.getFetchTime());
        break;
    }

    if (page.getInlinks() != null) {
      page.getInlinks().clear();
    }

    // Distance calculation.
    // Retrieve smallest distance from all inlinks distances
    // Calculate new distance for current page: smallest inlink distance plus 1.
    // If the new distance is smaller than old one (or if old did not exist yet),
    // write it to the page.
    int smallestDist=Integer.MAX_VALUE;
    for (ScoreDatum inlink : inlinkedScoreData) {
      int inlinkDist = inlink.getDistance();
      if (inlinkDist < smallestDist) {
        smallestDist=inlinkDist;
      }
      page.getInlinks().put(inlink.getUrl(), inlink.getAnchor());
    }

    if (smallestDist != Integer.MAX_VALUE) {
      int oldDistance=Integer.MAX_VALUE;
      String oldDistString = page.getMarkers().get(DbUpdaterJob.DISTANCE);
      if (oldDistString != null)oldDistance=Integer.parseInt(oldDistString);
      int newDistance = smallestDist+1;
      if (newDistance < oldDistance) {
        page.getMarkers().put(DbUpdaterJob.DISTANCE, Integer.toString(newDistance));
      }
    }

    try {
      scoringFilters.updateScore(url, page, inlinkedScoreData);
    } catch (ScoringFilterException e) {
      LOG.warn("Scoring filters failed with exception " +
        StringUtils.stringifyException(e));
    }

    if (page.getMetadata().get(FetcherJob.REDIRECT_DISCOVERED) != null) {
      page.getMetadata().put(FetcherJob.REDIRECT_DISCOVERED, null);
    }

    Mark.UPDATEDB_MARK.putMark(page, "y");

    context.write(key, page);
  }

}
