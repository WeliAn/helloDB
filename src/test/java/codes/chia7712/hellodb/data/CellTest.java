package codes.chia7712.hellodb.data;

import codes.chia7712.hellodb.Table;
import codes.chia7712.hellodb.admin.Admin;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestName;


public class CellTest {
  
  @Rule
  public TestName name = new TestName();
  private static final Properties PROP = new Properties();
  private static Admin ADMIN;
  private static final byte[] ROW_V0 = BytesUtil.toBytes("row_v0");
  private static final byte[] ROW_V1 = BytesUtil.toBytes("row_v1");
  private static final byte[] COLUMN_V0 = BytesUtil.toBytes("column_v0");
  private static final byte[] COLUMN_V1 = BytesUtil.toBytes("column_v1");
  private static final byte[] VALUE_V0 = BytesUtil.toBytes("value_v0");

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

  @Test
  public void testCellContent() throws IOException {
    ADMIN.createTable(name.getMethodName());
    Table t = ADMIN.openTable(name.getMethodName());
    Cell cell_v0 = Cell.createCell(ROW_V0, COLUMN_V0, VALUE_V0);
    Cell cell_v1 = Cell.createCell(ROW_V1, COLUMN_V0, VALUE_V0);
    Cell cell_v2 = Cell.createCell(ROW_V1, COLUMN_V1, VALUE_V0);
    t.insert(cell_v0);
    Optional<Cell> rval = t.get(ROW_V0, COLUMN_V0);
    assertTrue(rval.isPresent());
    assertEquals(0, BytesUtil.getComparator().compare(cell_v0, rval.get()));

    t.insert(cell_v1);
    t.insert(cell_v2);
    Iterator<Cell> cells = t.get(ROW_V1);
    int count = 0;
    while(cells.hasNext()) {
      Cell c = cells.next();
      if (count == 0) {
        assertEquals(0, BytesUtil.getComparator().compare(c, cell_v1));
      }
      if (count == 1) {
        assertEquals(0, BytesUtil.getComparator().compare(c, cell_v2));
      }
      ++count;
    }
    assertEquals(2, count);
  }
}
