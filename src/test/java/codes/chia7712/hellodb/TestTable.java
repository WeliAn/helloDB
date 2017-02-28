package codes.chia7712.hellodb;

import codes.chia7712.hellodb.admin.Admin;
import codes.chia7712.hellodb.data.BytesUtil;
import codes.chia7712.hellodb.data.Cell;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.rules.TestName;

public class TestTable {

  @Rule
  public TestName name = new TestName();
  private static final Properties PROP = new Properties();
  private static Admin ADMIN;
  private static final String TABLE_NAME = "TestTable";
  private static final byte[] ROW_V0 = BytesUtil.toBytes("row_v0");
  private static final byte[] VALUE_V0 = BytesUtil.toBytes("value_vB");

  @BeforeClass
  public static void setUpClass() throws Exception {
    PROP.put(Admin.ADMIN_IMPL, Admin.DEFAULT_ADMIN_IMPL);
    ADMIN = Admin.create(PROP);
//    ADMIN.createTable(TABLE_NAME);
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
   * Test of getName method, of class Table.
   */
  @Test
  public void testGetName() throws IOException {
    Table t = ADMIN.openTable(TABLE_NAME);
    assertEquals(t.getName(), TABLE_NAME);
  }

  /**
   * Test of insert method, of class Table.
   */
  @Test
  public void testInsert() throws Exception {
    Table t = ADMIN.openTable(TABLE_NAME);
    Cell newCell = Cell.createCell(ROW_V0, BytesUtil.toBytes("B"), VALUE_V0);
    int firstCount = count(t.get(ROW_V0));
    t.insert(newCell);
    assertEquals(1 + firstCount, count(t.get(ROW_V0)));
  }

  /**
   * Test of delete method, of class Table.
   */
  @Test
  public void testGet() throws Exception {
    Table t = ADMIN.openTable(TABLE_NAME);
    int firstCount = count(t.get(ROW_V0));
    Cell newCell = Cell.createCell(ROW_V0, BytesUtil.toBytes(name.getMethodName()), VALUE_V0);
    t.insert(newCell);
    assertEquals(1 + firstCount, count(t.get(ROW_V0)));
    Optional<Cell> rval = t.get(ROW_V0, BytesUtil.toBytes(name.getMethodName()));
    assertTrue(rval.isPresent());
    assertEquals(0, BytesUtil.getComparator().compare(rval.get(), newCell));
  }

  @Test
  public void testGetOp() throws Exception {
    Table t = ADMIN.openTable(TABLE_NAME);
//    Optional<Cell> o = t.get(ROW_V0, BytesUtil.toBytes("AGE"));
//    System.out.println("col = " + new String(o.get().getColumnArray())
//            + ", value = " + new String(o.get().getValueArray()));
    Iterator<Cell> cells = t.get(ROW_V0);
    while (cells.hasNext()) {
      Cell c = cells.next();
      System.out.println("col = " + new String(c.getColumnArray())
              + ", value = " + new String(c.getValueArray()));
    }
  }

  /**
   * Test of delete method, of class Table.
   */
  @Test
  public void testDelete_byteArr_byteArr() throws Exception {
    Table t = ADMIN.openTable(TABLE_NAME);
    assertFalse(t.delete(ROW_V0, BytesUtil.toBytes(name.getMethodName())));
    int firstCount = count(t.get(ROW_V0));
    t.insert(Cell.createCell(ROW_V0, BytesUtil.toBytes(name.getMethodName()), VALUE_V0));
    assertEquals(1 + firstCount, count(t.get(ROW_V0)));
    assertTrue(t.delete(ROW_V0, BytesUtil.toBytes(name.getMethodName())));
    assertEquals(firstCount, count(t.get(ROW_V0)));
  }

  /**
   * Test of insertIfAbsent method, of class Table.
   */
  @Test
  public void testInsertIfAbsent() throws Exception {
    Table t = ADMIN.openTable(TABLE_NAME);
    Cell newCell = Cell.createCell(ROW_V0, BytesUtil.toBytes((name.getMethodName())), VALUE_V0);
    int firstCount = count(t.get(ROW_V0));
    assertTrue(t.insertIfAbsent(newCell));
    assertEquals(1 + firstCount, count(t.get(ROW_V0)));
    assertFalse(t.insertIfAbsent(newCell));
    assertEquals(1 + firstCount, count(t.get(ROW_V0)));
  }

  private static int count(Iterator<Cell> cells) {
    int count = 0;
    while (cells.hasNext()) {
      cells.next();
      ++count;
    }
    return count;
  }
}
