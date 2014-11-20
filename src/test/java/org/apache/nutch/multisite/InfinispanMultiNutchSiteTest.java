package org.apache.nutch.multisite;

import org.apache.gora.GoraTestDriver;
import org.apache.gora.infinispan.GoraInfinispanTestDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.URLWebPage;
import org.apache.nutch.storage.Link;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
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
    List<String> cacheNames = new ArrayList<>();
    cacheNames.add(WebPage.class.getSimpleName());
    cacheNames.add(Link.class.getSimpleName());
    return new GoraInfinispanTestDriver(numberOfSites(),numberOfNodes(),cacheNames);
  }

  @Override
  protected int numberOfSites() {
    return 1;
  }

  @Override
  protected int numberOfNodes() {
    return 1;
  }

  @Override
  protected int partitionSize() {
    return 10000;
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
  public void inject() throws Exception {

    List<String> urls = new ArrayList<String>();
    for (int i = 0; i < 100; i++) {
      urls.add("http://zzz.com/" + i + ".html\tnutch.score=0");
    }

    site(0).inject(urls).get();
    assertEquals(100,readPageDB(null).size());

  }

  @Test
  public void generate() throws Exception {
    List<String> urls = new ArrayList<String>();
    for (int i = 0; i < 100; i++) {
      urls.add("http://zzz.com/" + i + ".html\tnutch.score=0");
    }

    // inject
    site(0).inject(urls).get();

    // generate
    List<Future<String>> futures = new ArrayList<>();
    for(NutchSite site : sites)
      futures.add(site
        .generate(0, System.currentTimeMillis(), false, false));

    // check result
    for(Future<String> future : futures)
      future.get();

    assertEquals(
     100,
     readPageDB(null).size());
  }

  @Test
  public void fetch() throws Exception {

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
        site.generate(0, System.currentTimeMillis(), false, false));

    // fetch
    long time = System.currentTimeMillis();
    List<Future<Integer>> futures = new ArrayList<>();
    for(NutchSite site : sites) {
      String batchId = batchIds.get(site).get();
      futures.add(
        site.fetch(batchId, 4, false, 0));
    }

    // check results
    for(Future<Integer> future : futures)
      future.get();

    // verify politeness, time taken should be more than (num_of_pages +1)*delay
    int minimumTime = (int) ((urls.size() + 1) * 1000 *
      conf.getFloat("fetcher.server.delay", 5));
    assertTrue((System.currentTimeMillis()-time) > minimumTime);

    // verify that enough pages were handled
    List<URLWebPage> pages = readPageDB(Mark.FETCH_MARK);
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

  @Test
  public void crawl() throws Exception {

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

    // inject
    site(0).inject(urls).get();

    assertEquals(3,readPageDB(null).size());

    // generate
    Map<NutchSite,Future<String>> batchIds =  new HashMap<>();
    for(NutchSite site : sites)
      batchIds.put(
        site,
        site.generate(0, System.currentTimeMillis(), false, false));

    // fetch
    List<Future<Integer>> futures = new ArrayList<>();
    for(NutchSite site : sites) {
      String batchId = batchIds.get(site).get();
      futures.add(
        site.fetch(batchId, 4, false, 0));
    }
    for(Future<Integer> future : futures)
      future.get();

    assertEquals(3,readPageDB(Mark.FETCH_MARK).size());

    // parse
    futures.clear();
    for(NutchSite site : sites) {
      String batchId = batchIds.get(site).get();
      futures.add(
        site.parse(batchId, false, false));
    }
    for(Future<Integer> future : futures)
      future.get();

    // verify content
    assertEquals(5,readLinkDB().size());
    assertEquals(4,readPageDB(null).size());

    // update pageDB
    futures.clear();
    for(NutchSite site : sites) {
      String batchId = batchIds.get(site).get();
      futures.add(
        site.update(batchId));
    }
    for(Future<Integer> future : futures)
      future.get();

    // verify content
    List<URLWebPage> pages = readPageDB(null);
    assertEquals(4,pages.size());
    for (URLWebPage urlWebPage : pages) {
      if (urlWebPage.getUrl().contains("dup"))
        assertEquals(0,urlWebPage.getDatum().getInlinks().size());
      else if (urlWebPage.getUrl().contains("page"))
        assertEquals(1,urlWebPage.getDatum().getInlinks().size());
      else // index
        assertEquals(2,urlWebPage.getDatum().getInlinks().size());
    }

    server.stop();

  }

  // Helpers

  public List<URLWebPage> readPageDB(Mark requiredMark, String... fields)
    throws Exception {
    List<URLWebPage> content = new ArrayList<>();
    for(NutchSite site : sites){
      content.addAll(site.readPageDB(requiredMark, fields));
    }
    return content;
  }

  public List<Link> readLinkDB()
    throws Exception {
    List<Link> links = new ArrayList<>();
    for(NutchSite site : sites){
      links.addAll(site.readLinkDB());
    }
    return links;
  }

}
