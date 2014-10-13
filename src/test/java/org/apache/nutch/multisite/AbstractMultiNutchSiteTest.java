package org.apache.nutch.multisite;

import org.apache.gora.GoraTestDriver;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Pierre Sutra
 */
public abstract class AbstractMultiNutchSiteTest {

  protected GoraTestDriver driver;
  protected List<NutchSite> sites;
  protected boolean isPersistent= false;

  public void setUpClass() throws Exception {
    driver = createDriver();
    driver.setUpClass();
    sites = new ArrayList<>();
    for(int i=0; i<numberOfSites(); i++) {
      Path path = new Path("build/test/working"+i);
      NutchSite site = new NutchSite(path,Integer.toString(i),isPersistent,connectionString(i));
      site.setUpClass();
      sites.add(site);
    }
  }

  public void tearDownClass() throws Exception {
    for (NutchSite site : sites)
      site.tearDownClass();
    driver.tearDownClass();
  }

  protected abstract GoraTestDriver createDriver();
  protected abstract int numberOfSites();
  protected abstract String connectionString(int i);

  // Helpers

  protected NutchSite site(int i){
    return sites.get(i);
  }


}
