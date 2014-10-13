package org.apache.nutch.multisite;

import org.apache.gora.GoraTestDriver;
import org.apache.gora.infinispan.GoraInfinispanTestDriver;
import org.apache.nutch.crawl.URLWebPage;
import org.apache.nutch.storage.Mark;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

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
    return 2;
  }

  @Override
  protected String connectionString(int i) {
    String ret="";
    String connectionString = ((GoraInfinispanTestDriver) driver).connectionString();
    String[] splits = connectionString.split("\\|");
    for(int j=0; j<splits.length; j++){
      String split = splits[j];
      if (j!=i)
        ret+=split+"|";
    }
    return ret+splits[i];
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
  public void injectionTest(){
    List<String> urls = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      urls.add("http://zzz.com/" + i + ".html\tnutch.score=0");
    }

    try {
      site(0).inject(urls);
      assert readContent(null).size() == 10;
    } catch (Exception e) {
      e.printStackTrace();
    }
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
