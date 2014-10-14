package org.apache.nutch.multisite;

import org.apache.gora.GoraTestDriver;
import org.apache.gora.infinispan.GoraInfinispanTestDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.URLWebPage;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.CrawlTestUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.jetty.Server;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Future;

import static org.apache.nutch.fetcher.TestFetcher.addUrl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author PIerre Sutra
 */
public class InfinispanMultiNutchSiteTest extends AbstractMultiNutchSiteTest {

  @Override
  protected GoraTestDriver createDriver() {
    return new GoraInfinispanTestDriver(numberOfSites());
  }

  @Override
  protected int numberOfSites() {
    return 3;
  }

  @Override
  protected String connectionString(int i) {

    String ret="";

    String connectionString = ((GoraInfinispanTestDriver) driver).connectionString();
    String[] splits = connectionString.split("\\|");

    if (splits.length==1)
      return connectionString;

    for(int j=0; j<splits.length; j++){
      String split = splits[j];
      if (j!=i)
        ret+=split+"|";
    }

    return splits[i]+"|"+ret.substring(0, ret.length() - 1);
  }

  @Before
  @Override
  public void setUpClass() throws Exception {
    super.setUpClass();
  }

  @After
  @Override
  public void tearDownClass() throws Exception {
    super.tearDownClass();
  }

  @Test
  public void injectionTest() throws Exception {

    List<String> urls = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      urls.add("http://zzz.com/" + i + ".html\tnutch.score=0");
    }

    site(0).inject(urls).get();
    assertEquals(readContent(null).size(),10);

  }

  @Test
  public void generatorTest() throws Exception {
    List<String> urls = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      urls.add("http://zzz.com/" + i + ".html\tnutch.score=0");
    }

    // inject
    site(0).inject(urls).get();

    // generate
    List<Future<String>> futures = new ArrayList<>();
    for(NutchSite site : sites)
      futures.add(site
        .generate(Long.MAX_VALUE, System.currentTimeMillis(), false, false));

    // check result
    for(Future<String> future : futures)
      future.get();

    assertEquals(readContent(null).size(),10);
  }

  @Test
  public void fetchTest() throws Exception {

    Configuration conf = NutchConfiguration.create();

    // start content server
    Server server = CrawlTestUtil.getServer(conf
      .getInt("content.server.port",50000), "src/test/resources/fetch-test-site");
    server.start();

    // generate seed list
    ArrayList<String> urls = new ArrayList<>();
    addUrl(urls,"index.html",server);
    addUrl(urls,"pagea.html",server);
    addUrl(urls,"pageb.html",server);
    addUrl(urls,"dup_of_pagea.html",server);
    addUrl(urls,"nested_spider_trap.html",server);
    addUrl(urls,"exception.html",server);

    // inject
    site(0).inject(urls).get();

    // generate
    Map<NutchSite,Future<String>> batchIds =  new HashMap<>();
    for(NutchSite site : sites)
      batchIds.put(
        site,
        site.generate(Long.MAX_VALUE, System.currentTimeMillis(), false, false));

    // fetch
    long time = System.currentTimeMillis();
    List<Future<Integer>> futures = new ArrayList<>();
    for(NutchSite site : sites) {
      String batchId = batchIds.get(site).get();
      futures.add(
        site.fetch(batchId, 1, false, 1));
    }

    // check results
    for(Future<Integer> future : futures)
      future.get();

    // verify politeness, time taken should be more than (num_of_pages +1)*delay
    int minimumTime = (int) ((urls.size() + 1) * 1000 *
      conf.getFloat("fetcher.server.delay", 5));
    assertTrue((System.currentTimeMillis()-time) > minimumTime);

    //verify that enough pages and correct were handled
    List<URLWebPage> pages = readContent(Mark.FETCH_MARK);
    assertEquals(urls.size(), pages.size());
    List<String> handledurls = new ArrayList<>();
    for (URLWebPage up : pages) {
      ByteBuffer bb = up.getDatum().getContent();
      if (bb == null) {
        continue;
      }
      String content = Bytes.toString(bb);
      if (content.indexOf("Nutch fetcher test page")!=-1) {
        handledurls.add(up.getUrl());
      }
    }
    Collections.sort(urls);
    Collections.sort(handledurls);
    assertEquals(urls.size(), handledurls.size());
    assertTrue(handledurls.containsAll(urls));
    assertTrue(urls.containsAll(handledurls));

    server.stop();

  }

  // Helpers

  public List<URLWebPage> readContent(Mark requiredMark, String... fields)
    throws Exception {
    List<URLWebPage> content = new ArrayList<>();
    for(NutchSite site : sites){
      content.addAll(site.readContent(requiredMark,fields));
    }
    return content;
  }

}
