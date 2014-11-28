package org.apache.nutch.multisite;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Future;

import static org.apache.nutch.fetcher.TestFetcher.addUrl;
import static org.apache.nutch.util.ServerTestUtils.createPages;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author PIerre Sutra
 */
public class InfinispanMultiNutchSiteTest extends AbstractMultiNutchSiteTest {

  public static final Logger LOG
    = LoggerFactory.getLogger(InfinispanMultiNutchSiteTest.class);

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
    return 1000;
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
    for (int i = 0; i < nbGeneratedPages(); i++) {
      urls.add("http://zzz.com/" + i + ".html\tnutch.score=0");
    }

    site(0).inject(urls).get();
    assertEquals(100,readPageDB(null).size());

  }

  @Test
  public void generate() throws Exception {
    List<String> urls = new ArrayList<String>();
    for (int i = 0; i < nbGeneratedPages(); i++) {
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
      nbGeneratedPages(),
     readPageDB(null).size());
  }

  @Test
  public void frontier() throws Exception {

    String batchId = "0";
    Link.Builder linkBuilder = Link.newBuilder();
    for( int i=0; i<nbGeneratedLinks(); i++) {
      Link link = linkBuilder.build();
      link.setIn("http://foo.org");
      link.setOut("http://bar.org/" + i);
      link.setBatchId(batchId);
      link.setKey(link.getOut()+"--"+link.getIn());
      site(0).getLinkDB().put(link.getKey(), link);
    }
    site(0).getLinkDB().flush();

    List<Link> links = readLinkDB();
    assertEquals(nbGeneratedLinks(),links.size());

    // frontier
    List<Future<Integer>> futures = new ArrayList<>();
    for(NutchSite site : sites)
      futures.add(
        site.frontier(batchId));

    // check result
    for(Future<Integer> future : futures)
      future.get();

    assertEquals(
      nbGeneratedLinks(),
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
      if (content.contains("Nutch fetcher test page")) {
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
  public void shortCrawl() throws Exception {

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

    // update pageDB
    futures.clear();
    for(NutchSite site : sites) {
      String batchId = batchIds.get(site).get();
      futures.add(
        site.update(batchId));
    }
    for(Future<Integer> future : futures)
      future.get();

    assertEquals(3,readPageDB(Mark.UPDATEDB_MARK).size());

    // update Frontier
    futures.clear();
    for(NutchSite site : sites) {
      String batchId = batchIds.get(site).get();
      futures.add(
        site.frontier(batchId));
    }
    for(Future<Integer> future : futures)
      future.get();

    // verify content
    List<URLWebPage> pages = readPageDB(null);
    List<Link> links = readLinkDB();
    assertEquals(5,links.size());
    assertEquals(4,pages.size());
    for (URLWebPage urlWebPage : pages) {
      if (urlWebPage.getUrl().contains("dup"))
        assertEquals(0,urlWebPage.getDatum().getInlinks().size());
      else if (urlWebPage.getUrl().contains("page")) // pagea and pageb
        assertEquals(1,urlWebPage.getDatum().getInlinks().size());
      else // index
        assertEquals(2,urlWebPage.getDatum().getInlinks().size());
    }

    server.stop();

  }

  @Test
  public void longCrawl() throws  Exception{

    final int NPAGES = 1000;
    final int DEGREE = 20;

    final int INJECT = 100;
    final int DEPTH = 3;
    final int WIDTH = 1000;

    Configuration conf = NutchConfiguration.create();
    File tmpDir = Files.createTempDir();
    List<String> pages = createPages(NPAGES, DEGREE,
      tmpDir.getAbsolutePath());

    Server server = CrawlTestUtil.getServer(
      conf.getInt("content.server.port", 50000),
      tmpDir.getAbsolutePath());
    server.start();

    try {

      ArrayList<String> urls = new ArrayList<>();
      for (String page : pages) {
        if (urls.size()==INJECT) break;
        addUrl(urls, page, server);
      }
      sites.get(0).inject(urls).get();

      List<Future<Integer>> futures = new ArrayList<>();
      for (NutchSite site : sites) {
        futures.add(site.crawl(WIDTH, DEPTH));
      }
      for(Future<Integer> future : futures)
        future.get();

      List<URLWebPage> resultPages = readPageDB(Mark.UPDATEDB_MARK, "key");
      List<Link> resultLinks = readLinkDB();
      LOG.info("Pages: " + resultPages.size());
      LOG.info("Links: " + resultLinks.size());
      if (LOG.isDebugEnabled()) {
        for (URLWebPage urlWebPage : resultPages) {
          System.out.println(urlWebPage.getDatum().getKey());
        }
        for (Link link: resultLinks) {
          System.out.println(link.getKey());
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      server.stop();
      FileUtils.deleteDirectory(tmpDir);
    }

  }

  // Helpers

  public int nbGeneratedPages(){
    return 100;
  }

  public int nbGeneratedLinks(){
    return 1000;
  }

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
