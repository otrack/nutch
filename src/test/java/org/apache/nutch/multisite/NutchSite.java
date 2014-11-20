package org.apache.nutch.multisite;

import org.apache.gora.infinispan.store.InfinispanStore;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.crawl.DbUpdaterJob;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.InjectorJob;
import org.apache.nutch.crawl.URLWebPage;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParserJob;
import org.apache.nutch.storage.Link;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.CrawlTestUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
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
  private DataStore<String, WebPage> pageDB;
  private DataStore<String, Link> linkDB;
  private boolean isPersistent;
  private String partitionSize;

  public NutchSite(Path path, String siteName, boolean isPersistent, String connectionString, String partitionSize) throws IOException {
    this.testdir = path;
    this.siteName = siteName;
    this.isPersistent = isPersistent;
    this.connectionString = connectionString;
    this.partitionSize = partitionSize;
  }


  public void setUpClass() {
    LOG.info("Setting up site "+siteName);
    try {
      conf = NutchConfiguration.create();
      fs = FileSystem.get(conf);
      conf.set(Nutch.CRAWL_ID_KEY, siteName);
      conf.set(GORA_CONNECTION_STRING_KEY,connectionString);
      conf.set(InfinispanStore.PARTITION_SIZE_KEY,partitionSize);
      pageDB = StorageUtils.createStore(conf, String.class, WebPage.class);
      pageDB.deleteSchema();
      linkDB = StorageUtils.createStore(conf, String.class, Link.class);
      linkDB.deleteSchema();
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

  public Future<Integer> parse(final String batchId, final boolean shouldResume,
    final boolean force)
    throws Exception {
    return pool.submit(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        ParserJob parser = new ParserJob(conf);
        return parser.parse(batchId, shouldResume, force);
      }
    });
  }

  public Future<Integer>update(final String batchId)
    throws Exception {
    return pool.submit(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        DbUpdaterJob dbUpdaterJob= new DbUpdaterJob(conf);
        return dbUpdaterJob.update(batchId);
      }
    });
  }

  // Helpers

  public List<URLWebPage> readPageDB(Mark requiredMark, String... fields)
    throws Exception {
    return CrawlTestUtil.readPageDB(pageDB, requiredMark, fields);
  }

  public Collection<? extends Link> readLinkDB() throws Exception {
    return CrawlTestUtil.readLinkDB(linkDB);
  }
}
