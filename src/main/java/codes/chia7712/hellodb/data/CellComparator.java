package codes.chia7712.hellodb.data;

import java.util.Comparator;

public class CellComparator implements Comparator<Cell> {

  @Override
  public int compare(Cell left, Cell right) {
    int rval = compareRow(left, right);
    if (rval != 0) {
      return rval;
    }
    return compareColumn(left, right);
  }

  public static int compareRow(Cell left, Cell right) {
    return compare(left.getRowArray(), left.getRowOffset(), left.getRowLength(),
            right.getRowArray(), right.getRowOffset(), right.getRowLength());
  }

  public static int compareColumn(Cell left, Cell right) {
    return compare(left.getColumnArray(), left.getColumnOffset(), left.getColumnLength(),
            right.getColumnArray(), right.getColumnOffset(), right.getColumnLength());
  }

  public static int compare(byte[] left, int leftOffset, int leftLength,
          byte[] right, int rightOffset, int rightLength) {
    for (int i = leftOffset, j = rightOffset; i < leftLength && j < rightLength; ++i, ++j) {
      int a = (left[i] & 0xff);
      int b = (right[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return leftLength - rightLength;
  }
}
