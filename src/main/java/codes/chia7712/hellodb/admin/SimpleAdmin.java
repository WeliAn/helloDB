package codes.chia7712.hellodb.admin;

import codes.chia7712.hellodb.Table;
import codes.chia7712.hellodb.data.Cell;
import codes.chia7712.hellodb.data.CellComparator;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

class SimpleAdmin implements Admin {

  private final ConcurrentMap<String, Table> tables = new ConcurrentSkipListMap<>();
  private final static String TABLES_PATH = "F:\\helloDB\\";
  private final static String SLASH = "\\";
  File f;

  SimpleAdmin(Properties prop) throws IOException {
    f = new File(TABLES_PATH);
    String[] tablesDir = f.list();
    for (String table : tablesDir) {
      if (new File(TABLES_PATH + table).isDirectory()) {
        tables.put(table, new SimpleTable(table));
      }
    }
  }

  @Override
  public void createTable(String name) throws IOException {
    if (tables.containsKey(name)) {
      throw new IOException(name + " exists");
    }
    tables.computeIfAbsent(name, SimpleTable::new);
    f = new File(TABLES_PATH + name);
    f.mkdir();
  }

  @Override
  public boolean tableExist(String name) throws IOException {
    return tables.containsKey(name);
  }

  @Override
  public void deleteTable(String name) throws IOException {
    if (tables.remove(name) == null) {
      throw new IOException(name + " not found");
    }
    f = new File(TABLES_PATH + name);
    f.delete();
  }

  @Override
  public Table openTable(String name) throws IOException {
    Table t = tables.get(name);
    if (t == null) {
      throw new IOException(name + " not found");
    }
    return t;
  }

  @Override
  public List<String> listTables() throws IOException {
    return tables.keySet().stream().collect(Collectors.toList());
  }

  @Override
  public void close() throws IOException {
    tables.forEach((k, v) -> {
      try {
        v.close();
        v = null;
      } catch (IOException ex) {
        System.out.println("Table " + k + " close failed");
      }
    });
    tables.clear();
  }

  private static class SimpleTable implements Table {

    private static final CellComparator CELL_COMPARATOR = new CellComparator();
    private static final ConcurrentNavigableMap<Cell, Cell> data = new ConcurrentSkipListMap<>(CELL_COMPARATOR);
    private final String name;
    private static final String COMMA = ",";
    private static final ConcurrentMap<String, Cell> cells = new ConcurrentSkipListMap();
    private final RowIO rowIO = new RowIO();

    SimpleTable(final String name) {
      this.name = name;
    }

    @Override
    public boolean insert(Cell cell) throws IOException {
      StringBuilder path = new StringBuilder();
      path.append(TABLES_PATH).append(name).append(SLASH)
              .append(new String(cell.getRowArray())).append(SLASH);
      rowIO.readIndex(cell, path);
      String key = new String(cell.getColumnArray());
      if (cells.containsKey(key)) {
        rowIO.writeIfExits(path.toString() + "cell", key, cell);
        rowIO.writeIndexAndCell(cell, path.toString());
        return true;
      } else {
        rowIO.writeIfNotExits(path, cell.getColumnArray(),
                cell.getColumnLength(), cell.getValueArray(), cell.getValueLength());
        return false;
      }
//      return data.put(cell, cell) != null;
    }

    @Override
    public void delete(byte[] row) throws IOException {
      StringBuilder path = new StringBuilder();
      path.append(TABLES_PATH).append(name).append(SLASH).append(new String(row));
      File f = new File(path.toString());
      f.delete();
//      Cell rowOnlyCell = Cell.createRowOnly(row);
//      for (Map.Entry<Cell, Cell> entry : data.tailMap(rowOnlyCell).entrySet()) {
//        if (CellComparator.compareRow(entry.getKey(), rowOnlyCell) != 0) {
//          return;
//        } else {
//          data.remove(entry.getKey());
//        }
//      }
    }

    @Override
    public Iterator<Cell> get(byte[] row) throws IOException {
//      Cell rowOnlyCell = Cell.createRowOnly(row);
//      List<Cell> rval = new ArrayList<>();
      StringBuilder path = new StringBuilder();
      path.append(TABLES_PATH).append(name).append(SLASH).append(new String(row)).append(SLASH);
      rowIO.readIndex(row, path);
//      for (Map.Entry<Cell, Cell> entry : data.tailMap(rowOnlyCell).entrySet()) {
//        if (CellComparator.compareRow(entry.getKey(), rowOnlyCell) != 0) {
//          break;
//        } else {
//          rval.add(entry.getValue());
//        }
//      }
      return cells.values().iterator();
    }

    @Override
    public Optional<Cell> get(byte[] row, byte[] column) throws IOException {
      StringBuilder path = new StringBuilder();
      path.append(TABLES_PATH).append(name).append(SLASH).append(new String(row)).append(SLASH);
      rowIO.readIndex(row, path);
      return Optional.ofNullable(cells.get(new String(column)));
    }

    @Override
    public boolean delete(byte[] row, byte[] column) throws IOException {
      StringBuilder path = new StringBuilder();
      path.append(TABLES_PATH).append(name).append(SLASH).append(new String(row)).append(SLASH);
      rowIO.readIndex(row, path);
      String key = new String(column);
      if (cells.containsKey(key)) {
        cells.remove(key);
        rowIO.writeCellOnly(path.toString());
        return true;
      } else {
        return false;
      }
//      return data.remove(Cell.createRowColumnOnly(row, column)) != null;
    }

    @Override
    public boolean insertIfAbsent(Cell cell) throws IOException {
      StringBuilder sb = new StringBuilder();
      sb.append(TABLES_PATH).append(name).append(SLASH).append(new String(cell.getRowArray())).append(SLASH);
      rowIO.readIndex(cell, sb);
      if (cells.containsKey(new String(cell.getColumnArray()))) {
        return false;
      } else {
        rowIO.writeIfNotExits(sb, cell.getColumnArray(), cell.getColumnLength(), cell.getValueArray(), cell.getValueLength());
        return true;
      }
//      return data.putIfAbsent(cell, cell) == null;
    }

    @Override
    public void close() throws IOException {
      data.clear();
      cells.clear();
    }

    @Override
    public String getName() {
      return name;
    }

    private static void call(String v) {
      System.out.println(v);
    }

    private static void createRow(String row) throws IOException {
      File f = new File(row);
      if (!f.exists()) {
        f.mkdirs();
      }
      File cellF = new File(row + "cell");
      if (!cellF.exists()) {
        cellF.createNewFile();
      }
      File indexF = new File(row + "index");
      if (!indexF.exists()) {
        indexF.createNewFile();
      }
    }

    private class RowIO {

      private String[] indexData;

      public RowIO() {
      }

      private synchronized void writeCellOnly(String path) throws IOException {
        String indexPath = path + "index";
        try (FileOutputStream fos = new FileOutputStream(indexPath);
                BufferedOutputStream bos = new BufferedOutputStream(fos)) {
          StringBuilder indexOutput = new StringBuilder();
          cells.entrySet().stream().forEach(v -> indexOutput.append(v.getKey()).append(COMMA)
                  .append(v.getValue().getColumnOffset()).append(COMMA).append(v.getValue().getColumnLength())
                  .append(COMMA).append(v.getValue().getValueOffset()).append(COMMA)
                  .append(v.getValue().getValueLength()).append("\n"));
          call(indexOutput.toString());
          bos.write(indexOutput.toString().getBytes());
          bos.flush();
        }
      }

      private synchronized void writeIndexAndCell(Cell cell, String path) throws IOException {
        String cellPath = path + "cell";
        try (FileOutputStream fos = new FileOutputStream(cellPath, true);
                BufferedOutputStream bos = new BufferedOutputStream(fos)) {
          bos.write(cell.getColumnArray());
          bos.write(cell.getValueArray());
          bos.flush();
        }
        String indexPath = path + "index";
        try (FileOutputStream fos = new FileOutputStream(indexPath);
                BufferedOutputStream bos = new BufferedOutputStream(fos)) {
          StringBuilder indexOutput = new StringBuilder();
          cells.entrySet().stream().forEach(v -> indexOutput.append(v.getKey()).append(COMMA)
                  .append(v.getValue().getColumnOffset()).append(COMMA).append(v.getValue().getColumnLength())
                  .append(COMMA).append(v.getValue().getValueOffset()).append(COMMA)
                  .append(v.getValue().getValueLength()).append("\n"));
          bos.write(indexOutput.toString().getBytes());
          bos.flush();
        }
      }

      private synchronized void writeIfExits(String cellPath, String key, Cell cell) throws IOException {
//        String index = indexPath.toString() + "index";
//        String cellPath = indexPath.toString() + "cell";
        int cellLength = getCellByteLength(cellPath);
        Cell c = cells.get(key);
        cells.put(key, Cell.createCell(cell.getRowArray(), 0, cell.getRowLength(),
                cell.getColumnArray(), cellLength, cell.getColumnLength(),
                cell.getValueArray(), cellLength + cell.getColumnLength(), cell.getValueLength()));
//        Cell modCell = cells.get(key);
//        try (FileOutputStream fos = new FileOutputStream(index);
//                BufferedOutputStream bos = new BufferedOutputStream(fos)) {
//          StringBuilder sb = new StringBuilder();
//          sb.append(new String(modCell.getColumnArray())).append(COMMA)
//                  .append(modCell.getColumnOffset()).append(COMMA).append(modCell.getColumnLength())
//                  .append(COMMA).append(modCell.getValueOffset()).append(COMMA).append(modCell.getValueLength()).append("\n");
//          bos.write(sb.toString().getBytes());
//          bos.flush();
//        }
//        try (FileOutputStream fos = new FileOutputStream(cellPath, true);
//                BufferedOutputStream bos = new BufferedOutputStream(fos)) {
//          bos.write(modCell.getValueArray(), modCell.getValueOffset(), modCell.getValueLength());
//          bos.flush();
//        }
      }

      private void writeIfNotExits(StringBuilder indexPath, byte[] col, int colLength,
              byte[] valueArray, int valueLength) throws IOException {
        String cellPath = indexPath.toString() + "cell";
        int cellArraySize = getCellByteLength(cellPath);
        String index = indexPath.toString() + "index";
        try (FileOutputStream fos = new FileOutputStream(index, true);
                BufferedOutputStream bos = new BufferedOutputStream(fos)) {
          StringBuilder sb = new StringBuilder();
          sb.append(new String(col)).append(COMMA).append(cellArraySize).append(COMMA).append(colLength)
                  .append(COMMA).append((cellArraySize + colLength)).append(COMMA).append(valueLength).append("\n");
          bos.write(sb.toString().getBytes());
          bos.flush();
        }
        try (FileOutputStream fos = new FileOutputStream(cellPath, true);
                BufferedOutputStream bos = new BufferedOutputStream(fos)) {
          bos.write(col);
          bos.write(valueArray);
          bos.flush();
        }
      }

      private int getCellByteLength(String cellPath) throws IOException {
        int num = 0;
        try (FileInputStream fis = new FileInputStream(cellPath);
                BufferedInputStream bis = new BufferedInputStream(fis)) {
          byte[] b = new byte[1];
          while (bis.available() > 0) {
            bis.read(b);
            num++;
          }
        }
        return num;
      }

      private synchronized void readIndex(Cell cell, StringBuilder path) throws IOException {
        Cell c;
        byte[] valueArray;
        String line;
        String paths = path.toString();
        String index = paths + "index";
        String cellPath = paths + "cell";
        if (!(new File(index).exists())) {
          createRow(paths);
        }
        try (Scanner sc = new Scanner(new FileInputStream(index), "UTF-8");
                FileInputStream fis = new FileInputStream(cellPath);
                BufferedInputStream bis = new BufferedInputStream(fis)) {
          while (sc.hasNextLine()) {
            line = sc.nextLine();
            if (line.length() > 0) {
              IndexParse(line);
              valueArray = new byte[1024 * 1024];
              bis.read(valueArray, getValueOffset(), getValueLength());
              c = Cell.createCell(cell.getRowArray(), 0, cell.getRowLength(),
                      getColArray(), getColOffset(), getColLength(),
                      valueArray, getValueOffset(), getValueLength());
              data.put(c, c);
            }
          }
        }
      }

      private synchronized void readIndex(byte[] row, StringBuilder path) throws IOException {
        MappedByteBuffer in;
        Cell c;
        byte[] valueArray;
        String line;
        String paths = path.toString();
        String index = paths + "index";
        String cellPath = paths + "cell";
        if (!(new File(index).exists())) {
          createRow(paths);
        }
        try (Scanner sc = new Scanner(new FileInputStream(index), "UTF-8");
                RandomAccessFile raf = new RandomAccessFile(cellPath, "r")) {
          while (sc.hasNextLine()) {
            line = sc.nextLine();
            if (line.length() > 0) {
              IndexParse(line);
              valueArray = new byte[getValueLength()];
              in = raf.getChannel()
                      .map(FileChannel.MapMode.READ_ONLY, getValueOffset(), getValueLength());
              in.get(valueArray);
              c = Cell.createCell(row, 0, row.length,
                      getColArray(), getColOffset(), getColLength(),
                      valueArray, getValueOffset(), getValueLength());
              data.put(c, c);
              cells.put(new String(getColArray()), c);
            }
          }
        }
      }

      private void IndexParse(String line) {
        this.indexData = line.split(COMMA);
      }

      private byte[] getColArray() {
        return indexData[0].getBytes();
      }

      private int getColOffset() {
        if (indexData[1] == null) {
          return 0;
        }
        return Integer.valueOf(indexData[1]);
      }

      private int getColLength() {
        if (indexData[2] == null) {
          return 0;
        }
        return Integer.valueOf(indexData[2]);
      }

      private int getValueOffset() {
        if (indexData[3] == null) {
          return 0;
        }
        return Integer.valueOf(indexData[3]);
      }

      private int getValueLength() {
        if (indexData[4] == null) {
          return 0;
        }
        return Integer.valueOf(indexData[4]);
      }

    }
  }

}
