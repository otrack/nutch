package org.apache.nutch.multisite;

import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.InjectorJob;
import org.apache.nutch.crawl.URLWebPage;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.CrawlTestUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.gora.store.DataStoreFactory.GORA_CONNECTION_STRING_KEY;

/**
* @author PIerre Sutra
*/
public class NutchSite {

  public static final Logger LOG = LoggerFactory.getLogger(NutchSite.class);

  private static final ExecutorService pool
    = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

  private String siteName;
  private Configuration conf;

  private FileSystem fs;
  private Path testdir;

  private String connectionString;
  private DataStore<String, WebPage> store;
  private boolean isPersistent;

  public NutchSite(Path path, String siteName, boolean isPersistent, String connectionString) throws IOException {
    this.testdir = path;
    this.siteName = siteName;
    this.isPersistent = isPersistent;
    this.connectionString = connectionString;
  }


  public void setUpClass() {
    LOG.info("Setting up site "+siteName);
    try {
      conf = NutchConfiguration.create();
      fs = FileSystem.get(conf);
      conf.set(Nutch.CRAWL_ID_KEY, siteName);
      conf.set(GORA_CONNECTION_STRING_KEY,connectionString);
      store = StorageUtils
        .createWebStore(conf, String.class, WebPage.class);
      store.deleteSchema();
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("Site "+siteName+" creation failed", e);
      try {
        tearDownClass();
      } catch (IOException e1) {
        e1.printStackTrace();
      }
      throw new RuntimeException();
    }
  }

  public void tearDownClass() throws IOException {
    if (!isPersistent)
      fs.deleteOnExit(testdir);
  }

  public Future<Void> inject(final List<String> urls) throws Exception {
    return pool.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        InjectorJob injector = new InjectorJob(conf);
        Path urlPath = new Path(testdir, "urls");
        CrawlTestUtil.generateSeedList(fs, urlPath, urls);
        injector.inject(urlPath);
        return null;
      }
    });
  }

  public Future<String> generate(
    final long topN, final long curTime, final boolean useFiltering, final boolean normURL)
    throws Exception {
    return pool.submit(new Callable<String>() {
      @Override
      public String call() throws Exception {
        GeneratorJob g = new GeneratorJob(conf);
        String batchId = g.generate(topN, curTime, useFiltering, normURL);
        if (batchId == null)
          throw new RuntimeException("Generator failed");
        return batchId;

      }
    });
  }

  public Future<Integer> fetch(
    final String batchId, final int numThreads, final boolean shouldResume, final int numTasks)
    throws Exception {
    return pool.submit(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        FetcherJob fetcher = new FetcherJob(conf);
        return fetcher.fetch(batchId, numThreads, shouldResume, numTasks);
      }
    });
  }

  // Helpers

  public List<URLWebPage> readContent(Mark requiredMark, String... fields)
    throws Exception {
    return CrawlTestUtil.readContents(store, requiredMark, fields);
  }


}
