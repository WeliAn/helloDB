package codes.chia7712.hellodb.admin;

import java.io.IOException;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class TestAdmin {

  @Rule
  public TestName name = new TestName();
  private static final Properties PROP = new Properties();
  private static Admin ADMIN;

  @BeforeClass
  public static void setUpClass() throws Exception {
    PROP.put(Admin.ADMIN_IMPL, Admin.DEFAULT_ADMIN_IMPL);
    ADMIN = Admin.create(PROP);
  }

  @AfterClass
  public static void tearDownClass() throws IOException {
    ADMIN.close();
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  /**
   * Test of tableExist method, of class Admin.
   */
  @Test
  public void testTableExist() throws Exception {
    assertFalse(ADMIN.tableExist(name.getMethodName()));
    ADMIN.createTable(name.getMethodName());
    assertTrue(ADMIN.tableExist(name.getMethodName()));
  }

  /**
   * Test of deleteTable method, of class Admin.
   */
  @Test
  public void testDeleteTable() throws IOException {
    try {
      ADMIN.deleteTable(name.getMethodName());
      fail("It should be fail because we try to delete the non-existed table");
    } catch (IOException ex) {
    }
    ADMIN.createTable(name.getMethodName());
    ADMIN.deleteTable(name.getMethodName());
  }

  /**
   * Test of openTable method, of class Admin.
   */
  @Test
  public void testOpenTable() throws Exception {
    try {
      ADMIN.openTable(name.getMethodName());
      fail("It should be fail because we try to open the non-existed table");
    } catch (IOException ex) {
    }
    ADMIN.createTable(name.getMethodName());
    ADMIN.openTable(name.getMethodName());
  }

  /**
   * Test of listTables method, of class Admin.
   */
  @Test
  public void testListTables() throws Exception {
    for (String n : ADMIN.listTables()) {
      ADMIN.deleteTable(n);
    }
  }

}
