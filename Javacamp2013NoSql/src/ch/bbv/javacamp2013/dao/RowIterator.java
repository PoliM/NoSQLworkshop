package ch.bbv.javacamp2013.dao;

import java.nio.ByteBuffer;
import java.util.Iterator;

import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

/**
 * That iterator can be used to loop over the rows of a column family.<br>
 * It does not use the standard java iterator pattern. The iterator points to a
 * entry (as opposed to the standard java iterator, were the iterator points
 * more to a position between two entries). It uses a moveNext() method which
 * needs to be called first and every time the iterator shall be moved. Then
 * there are the getter, which read the values at the current position. This
 * getter can be called multiple times. Usage: <code>
 * <pre>
 *    RowIterator i = xy.getIterator();
 *    while (i.moveNext())
 *    {
 *       // Call the getter as many time as you want to retrieve
 *       // the values at the current position of the iterator.
 *    }
 * </pre>
 * </code>
 * 
 * @param <K> The type of the row key
 * @param <N> The type of the column names
 */
public class RowIterator<K, N> {
   private static final int MAX_COLS = 10;

   private static final int ROW_COUNT = 10000;

   private final RangeSlicesQuery<K, N, ByteBuffer> rangeSlicesQuery;

   private OrderedRows<K, N, ByteBuffer> rows;

   private Iterator<Row<K, N, ByteBuffer>> rowsIterator;

   private Row<K, N, ByteBuffer> row;

   /**
    * Creates a new iterator.
    * 
    * @param keyspace The keyspace of the db.
    * @param columnFamilyName For which column family.
    * @param keySerializer The serializer of the key.
    * @param nameSerializer The seciralizer of the names.
    */
   protected RowIterator(final Keyspace keyspace, final String columnFamilyName, final Serializer<K> keySerializer,
         final Serializer<N> nameSerializer) {
      this.rangeSlicesQuery = HFactory.createRangeSlicesQuery(keyspace, keySerializer, nameSerializer,
            ByteBufferSerializer.get());

      this.rangeSlicesQuery.setColumnFamily(columnFamilyName);
      this.rangeSlicesQuery.setRange(null, null, false, MAX_COLS);
      this.rangeSlicesQuery.setRowCount(ROW_COUNT);

      loadRowsPaged();
   }

   private void loadRowsPaged() {
      final K keyOfLastRow = (this.row == null) ? null : this.row.getKey();

      this.rangeSlicesQuery.setKeys(keyOfLastRow, null);
      // System.out.println(" > " + keyOfLastRow);

      final QueryResult<OrderedRows<K, N, ByteBuffer>> result = this.rangeSlicesQuery.execute();
      this.rows = result.get();
      this.rowsIterator = this.rows.iterator();

      // we'll skip this first one, since it is the same as the last one
      // from previous time we executed
      if (keyOfLastRow != null && this.rowsIterator != null) {
         this.rowsIterator.next();
      }
   }

   /**
    * Moves the iterator to the next row.
    * 
    * @return true if it now points to a row, else it returns false.
    */
   public final boolean moveNext() {
      // check if we have read all the "paged" rows
      if (!this.rowsIterator.hasNext()) {
         // if that was the last page, we are done, return false
         if (this.rows.getCount() < ROW_COUNT) {
            this.row = null;
            return false;
         }
         else {
            this.loadRowsPaged();
         }
      }
      this.row = this.rowsIterator.next();
      return true;
   }

   /**
    * Moves the iterator to the next row and skips empty column.
    * 
    * @return true if it now points to a row, else it returns false.
    */
   public final boolean moveNextSkipEmptyRow() {
      while (this.moveNext()) {
         if (!this.row.getColumnSlice().getColumns().isEmpty()) {
            return true;
         }
      }
      return false;
   }

   /**
    * @return The key of the current row.
    */
   public final K getKey() {
      return this.row.getKey();
   }

   /**
    * Gives a value of a "cell".
    * 
    * @param columnName The column name.
    * @return the value
    */
   public final ByteBuffer getValueByColumnName(final N columnName) {
      return this.row.getColumnSlice().getColumnByName(columnName).getValue();
   }
}