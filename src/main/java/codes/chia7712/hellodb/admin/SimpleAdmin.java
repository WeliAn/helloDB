package codes.chia7712.hellodb.admin;

import codes.chia7712.hellodb.Table;
import codes.chia7712.hellodb.data.BytesUtil;
import codes.chia7712.hellodb.data.Cell;
import codes.chia7712.hellodb.data.CellComparator;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

class SimpleAdmin implements Admin {

  private final ConcurrentMap<String, Table> tables = new ConcurrentSkipListMap<>();
  private final static String TABLES_PATH = "welian/";
  private final static String SLASH = "/";
  File f;

  SimpleAdmin(Properties prop) throws IOException {
    f = new File(TABLES_PATH);
    if (f.exists()) {
      String[] tablesDir = f.list();
      for (String table : tablesDir) {
        if (new File(TABLES_PATH + table).isDirectory()) {
          tables.put(table, new SimpleTable(table));
        }
      }
    } else {
      f.mkdirs();
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
      } catch (IOException ex) {
        System.out.println("Table " + k + " close failed");
      }
    });
    tables.clear();
  }

  private static class SimpleTable implements Table {

//    private static final CellComparator CELL_COMPARATOR = new CellComparator();
//    private static final ConcurrentNavigableMap<Cell, Cell> data = new ConcurrentSkipListMap<>(CELL_COMPARATOR);
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
      String key = new String();
      rowIO.readIndex(cell, path);
      synchronized (key) {
        key = new String(cell.getColumnArray());
        if (cells.containsKey(key)) {
          rowIO.writeIfExits(path.toString() + "cell", key, cell);
          rowIO.writeIndexAndCell(cell, path.toString());
          return true;
        } else {
          rowIO.writeIfNotExits(path, cell.getColumnArray(),
                  cell.getColumnLength(), cell.getValueArray(), cell.getValueLength());
          return false;
        }
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
      StringBuilder path = new StringBuilder();
      path.append(TABLES_PATH).append(name).append(SLASH).append(new String(row)).append(SLASH);
      rowIO.readIndex(row, path);
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
      String key = new String();;
      synchronized (key) {
        key = new String(column);
        if (cells.containsKey(key)) {
          cells.remove(key);
          rowIO.writeCellOnly(path.toString());
          return true;
        } else {
          return false;
        }
      }
    }

    @Override
    public boolean insertIfAbsent(Cell cell) throws IOException {
      StringBuilder sb = new StringBuilder();
      sb.append(TABLES_PATH).append(name).append(SLASH).append(new String(cell.getRowArray())).append(SLASH);
      rowIO.readIndex(cell, sb);
      cells.keySet().stream().forEach(System.out::println);
      if (cells.containsKey(new String(cell.getColumnArray()))) {
        return false;
      } else {
        rowIO.writeIfNotExits(sb, cell.getColumnArray(), cell.getColumnLength(), cell.getValueArray(), cell.getValueLength());
        return true;
      }
    }

    @Override
    public void close() throws IOException {
      cells.clear();
    }

    @Override
    public String getName() {
      return name;
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

      public RowIO() {
      }

      private synchronized void writeCellOnly(String path) throws IOException {
        String indexPath = path + "index";
        try (RandomAccessFile raf = new RandomAccessFile(indexPath, "rw")) {
          raf.writeInt(cells.size());
          for (Map.Entry<String, Cell> v : cells.entrySet()) {
            raf.writeInt(v.getKey().length());
            raf.write(v.getValue().getColumnArray());
            raf.writeInt(v.getValue().getColumnOffset());
            raf.writeInt(v.getValue().getColumnLength());
            raf.writeInt(v.getValue().getValueOffset());
            raf.writeInt(v.getValue().getValueLength());
          }
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
        try (RandomAccessFile raf = new RandomAccessFile(indexPath, "rw")) {
          raf.seek(0);
          raf.writeInt(cells.size());
          for (Map.Entry<String, Cell> v : cells.entrySet()) {
            raf.writeInt(v.getKey().length());
            raf.write(v.getValue().getColumnArray());
            raf.writeInt(v.getValue().getColumnOffset());
            raf.writeInt(v.getValue().getColumnLength());
            raf.writeInt(v.getValue().getValueOffset());
            raf.writeInt(v.getValue().getValueLength());
          }
        }
      }

      private synchronized void writeIfExits(String cellPath, String key, Cell cell) throws IOException {
        int cellLength = getCellByteLength(cellPath);
        Cell c = cells.get(key);
        cells.put(key, Cell.createCell(cell.getRowArray(), 0, cell.getRowLength(),
                cell.getColumnArray(), cellLength, cell.getColumnLength(),
                cell.getValueArray(), cellLength + cell.getColumnLength(), cell.getValueLength()));
      }

      private void writeIfNotExits(StringBuilder indexPath, byte[] col, int colLength,
              byte[] valueArray, int valueLength) throws IOException {
        String cellPath = indexPath.toString() + "cell";
        int cellArraySize = getCellByteLength(cellPath);
        String index = indexPath.toString() + "index";
        try (RandomAccessFile raf = new RandomAccessFile(index, "rw")) {
          raf.seek(0);
          int cellsize = cells.size() + 1;
          raf.writeInt(cellsize);
          System.out.println("cell size = " + cellsize);
          raf.seek(raf.length());
          System.out.println("col = " + new String(col));
          raf.writeInt(colLength);
          raf.write(col);
          raf.writeInt(cellArraySize);
          raf.writeInt(colLength);
          raf.writeInt((cellArraySize + colLength));
          raf.writeInt(valueLength);
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
        String paths = path.toString();
        byte[] valueArray;
        Cell c;
        String index = paths + "index";
        String cellPath = paths + "cell";
        if (!(new File(index).exists())) {
          createRow(paths);
        }
        try (RandomAccessFile raf = new RandomAccessFile(index, "r");
                FileInputStream fis = new FileInputStream(cellPath);
                BufferedInputStream bis = new BufferedInputStream(fis)) {
          if (raf.length() > 0) {
            int colNum = raf.readInt();
            while (colNum > 0) {
              int colByte = raf.readInt();
              byte[] col = new byte[colByte];
              raf.read(col);
              int coloffset = raf.readInt();
              int colLength = raf.readInt();
              int voffset = raf.readInt();
              int vLength = raf.readInt();
              valueArray = new byte[1024 * 1024];
              bis.read(valueArray, voffset, vLength);
              c = Cell.createCell(cell.getRowArray(), 0, cell.getRowLength(),
                      col, coloffset, colLength,
                      valueArray, voffset, vLength);
              colNum--;
              cells.put(new String(col), c);
            }
          }
        }
      }

      private synchronized void readIndex(byte[] row, StringBuilder path) throws IOException {
        MappedByteBuffer in;
        Cell c;
        byte[] valueArray;
        String paths = path.toString();
        String index = paths + "index";
        String cellPath = paths + "cell";
        if (!(new File(index).exists())) {
          createRow(paths);
        }
        try (RandomAccessFile raf = new RandomAccessFile(index, "r");
                FileInputStream fis = new FileInputStream(cellPath);
                BufferedInputStream bis = new BufferedInputStream(fis)) {
          if (raf.length() > 0) {
            int colNum = raf.readInt();
            while (colNum > 0) {
              int colByte = raf.readInt();
              byte[] col = new byte[colByte];
              raf.read(col);
              int coloffset = raf.readInt();
              int colLength = raf.readInt();
              int voffset = raf.readInt();
              int vLength = raf.readInt();
              valueArray = new byte[1024 * 1024];
              bis.read(valueArray, voffset, vLength);
              c = Cell.createCell(row, 0, row.length,
                      col, coloffset, colLength,
                      valueArray, voffset, vLength);
              colNum--;
              cells.put(new String(col), c);
            }
          }
        }
      }
    }
  }

}
