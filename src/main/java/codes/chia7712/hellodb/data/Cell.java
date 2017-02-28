package codes.chia7712.hellodb.data;

public interface Cell {

  public static Cell createRowOnly(byte[] row) {
    return new SimpleCell(row, 0, row.length,
            null, 0, 0,
            null, 0, 0);
  }

  public static Cell createRowColumnOnly(byte[] row, byte[] column) {
    return new SimpleCell(row, 0, row.length,
            column, 0, column.length,
            null, 0, 0);
  }

  public static Cell createCell(byte[] row, byte[] column, byte[] value) {
    return new SimpleCell(row, 0, row.length,
            column, 0, column.length,
            value, 0, value.length);
  }

  public static Cell createCell(final byte[] rowArray, final int rowOffset, final int rowLength,
          final byte[] columnArray, final int columnOffset, final int columnLength,
          final byte[] valueArray, final int valueOffset, final int valueLength) {
    return new SimpleCell(rowArray, rowOffset, rowLength,
            columnArray, columnOffset, columnLength,
            valueArray, valueOffset, valueLength);
  }

  byte[] getRowArray();

  int getRowOffset();

  int getRowLength();

  byte[] getColumnArray();

  int getColumnOffset();

  int getColumnLength();

  byte[] getValueArray();

  int getValueOffset();

  int getValueLength();
}
