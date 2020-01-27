package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TreeMap;

/**
 * IndexedRowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 *
 * Also has a tree index on column `indexColumn`, which points
 * to all row indices with the given value.
 */
public class IndexedRowTable implements Table {

    int numCols;
    int numRows;
    private TreeMap<Integer, IntArrayList> index;
    private ByteBuffer rows;
    private int indexColumn;

    public IndexedRowTable(int indexColumn) {
        this.indexColumn = indexColumn;
        index = new TreeMap<>();

    }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                int field = curRow.getInt(ByteFormat.FIELD_LEN * colId);

                if (colId == indexColumn) {
                    addToTree(rowId, field);
                }
                this.rows.putInt(offset, field);
            }
        }
    }


    private void addToTree(int rowId, int field) {
        if (index.containsKey(field)) {
            IntArrayList listRows = index.get(field);
            listRows.add(rowId);
        } else {
            IntArrayList listRows = new IntArrayList();
            listRows.add(rowId);
            index.put(field, listRows);
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        return rows.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        if (colId == indexColumn) {
            int oldVal = this.rows.getInt(offset);
            if(oldVal == field) return;
            // remove old value
            IntArrayList deleteList = index.get(oldVal);
            deleteList.rem(rowId);
            if (deleteList.isEmpty()) {
              index.remove(oldVal);
            }

            // add new value
            addToTree(rowId, field);

        }
        this.rows.putInt(offset, field);

    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        int sum =0;
        for(int i =0; i < numRows; i++) {
            sum += getIntField(i, 0);
        }
        return sum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
     *
     *  Returns the sum of all elements in the first column of the table,
     *  subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        int sum =0;
        List<Integer> indexes = new ArrayList<>();
        List<IntArrayList> values;
        for (int i =0; i < numRows; i++) {
            indexes.add(i);
        }

        if (indexColumn == 1) {
           SortedMap<Integer, IntArrayList> sortedMap = index.tailMap(threshold1+1);
             values = new ArrayList<>(sortedMap.values());
            indexes.clear();
            for (IntArrayList intArrayList : values) {
                indexes.addAll(intArrayList);
            }
        }

        if (indexColumn == 2) {
            SortedMap<Integer, IntArrayList> sortedMap = index.headMap(threshold1);
            values = new ArrayList<>(sortedMap.values());
            indexes.clear();
            for (IntArrayList intArrayList : values) {
                indexes.addAll(intArrayList);
            }
        }

        for(Integer i : indexes) {
            int col1 = getIntField(i, 1);
            int col2 = getIntField(i, 2);
            if (col1 > threshold1 && col2 < threshold2) {
                sum += getIntField(i, 0);
            }
        }
        return sum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        int sum =0;
        List<Integer> indexes = new ArrayList<>();
        List<IntArrayList> values;

        if(indexColumn != 0) {
            for (int i = 0; i < numRows; i++) {
                indexes.add(i);
            }
        } else {
            SortedMap<Integer, IntArrayList> sortedMap
                = index.tailMap(threshold+1);
            values = new ArrayList<>(sortedMap.values());
            for (IntArrayList intArrayList : values) {
                indexes.addAll(intArrayList);
            }
        }

        for(int row : indexes) {
            int col0 = getIntField(row, 0);
            if(col0 > threshold) {
                for (int col =0; col < numCols; col++) {
                    sum += getIntField(row, col);
                }
            }
        }
        return sum;
    }

    /**
     * Implements the query
     *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        int count = 0;

        List<Integer> indexes = new ArrayList<>();
        List<IntArrayList> values;

        if(indexColumn != 0) {
            for (int i = 0; i < numRows; i++) {
                indexes.add(i);
            }
        } else {
            SortedMap<Integer, IntArrayList> sortedMap
                = index.headMap(threshold);
            values = new ArrayList<>(sortedMap.values());
            for (IntArrayList intArrayList : values) {
                indexes.addAll(intArrayList);
            }
        }

        for (int row : indexes) {
            int col0 = getIntField(row, 0);
            if( col0 < threshold) {
                count++;
                int sumVal = getIntField(row, 3) + getIntField(row, 2);
                putIntField(row, 3, sumVal);
            }
        }
        return count;
    }
}
