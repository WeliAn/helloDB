package codes.chia7712.hellodb.admin;

import codes.chia7712.hellodb.Table;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Properties;

public interface Admin extends Closeable {
  public static final String ADMIN_IMPL = "admin.impl";
  public static final String DEFAULT_ADMIN_IMPL = SimpleAdmin.class.getName();
  public static Admin create(Properties properties) throws Exception {
    String clzName = properties.getProperty(ADMIN_IMPL);
    if (clzName == null) {
      throw new IllegalArgumentException("Failed to find the admin implementation");
    }
    Class<?> clazz = Class.forName(clzName);
    Constructor<?> ctor = clazz.getDeclaredConstructor(Properties.class);
    ctor.setAccessible(true);
    return (Admin) ctor.newInstance(properties);
  }

  void createTable(String name) throws IOException;

  boolean tableExist(String name) throws IOException;

  void deleteTable(String name) throws IOException;

  Table openTable(String name) throws IOException;

  List<String> listTables() throws IOException;

}
