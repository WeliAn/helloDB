
import codes.chia7712.hellodb.Table;
import codes.chia7712.hellodb.data.BytesUtil;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Scanner;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Weli
 */
public class TestWriteByte {

  public static void main(String[] args) throws IOException, ClassNotFoundException {
    ConcurrentMap<String, String> cells = new ConcurrentSkipListMap();
    cells.put("A", "1");
    cells.put("B", "2");
    cells.put("C", "3");
    StringBuilder cellOutput = new StringBuilder();
    cells.entrySet().stream().forEach(v -> cellOutput.append(v.getKey())
            .append(v.getValue()));
    System.out.println(cellOutput.toString());

//    try (Scanner sc = new Scanner(new FileInputStream("F:\\helloDB\\TestTable\\row_v0\\index"), "UTF-8")) {
//      while (sc.hasNextLine()) {
//        String line = sc.nextLine();
//        System.out.println(line);
//      }
//    }
//    final String ROW = "row1";
//    final String COL = "col1";
//    final String VALUE = "value1";
//    ConcurrentMap<String, String> tables = new ConcurrentSkipListMap<>();
//    byte[] buf = BytesUtil.toBytes(ROW);
//    File f = new File("F:\\helloDB\\table");
//    if (!f.exists()) {
//      f.mkdirs();
//    }
//    f = new File("F:\\helloDB\\table\\test");
//    if (f.exists() && f.isFile()) {
//      MappedByteBuffer out = new RandomAccessFile("F:\\helloDB\\table\\test", "rw").getChannel()
//              .map(FileChannel.MapMode.READ_WRITE, 0, buf.length);
//      out.put(buf);
//    } else {
//      try (FileOutputStream fos = new FileOutputStream("F:\\helloDB\\table\\test")) {
//        byte[] b = new byte[0];
//        fos.write(b);
//      }
//    }
//    byte[] all = new byte[1024];
//    out.get(all, 0, 10);
//    out.clear();
//    System.out.println(new String(all));
//    out.get(buf, 0, 5);
//    System.out.println(new String(buf));
//    try (FileOutputStream fos = new FileOutputStream("F:\\helloDB\\tables");
//            BufferedOutputStream bos = new BufferedOutputStream(fos);
//            ObjectOutputStream oos = new ObjectOutputStream(bos)) {
//      byte[] buf;
//      tables.put("B", "baby");
//      tables.put("K", "kk");
//      tables.put("Y", "yes");
//      tables.put("A","123");
//      oos.writeObject(tables);
//      oos.flush();
////      StringBuilder cell = new StringBuilder();
////      cell.append(ROW).append(COL).append(VALUE);
////      buf = BytesUtil.toBytes(cell.toString());
////      bos.write(buf, 0, 14);
////      bos.flush();
//    }
//    try (FileInputStream fis = new FileInputStream("F:\\helloDB\\tables");
//            BufferedInputStream bis = new BufferedInputStream(fis);
//            ObjectInputStream ois = new ObjectInputStream(bis)) {
////      byte[] buffer = new byte[1024];
////      bis.read(buffer, 0, 3);
////      System.out.println(new String(buffer));
////      while (fis.available() > 0) {
//        tables = (ConcurrentMap<String, String>) ois.readObject();
//        tables.keySet().stream().forEach(System.out::println);
////      }
//    }
  }
}
