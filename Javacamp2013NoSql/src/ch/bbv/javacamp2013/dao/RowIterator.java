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
public class RowIterator<K, N>
{
   private static final int ROW_COUNT = 100;

   private final RangeSlicesQuery<K, N, ByteBuffer> rangeSlicesQuery;

   private OrderedRows<K, N, ByteBuffer> rows;

   private Iterator<Row<K, N, ByteBuffer>> rowsIterator;

   private Row<K, N, ByteBuffer> row;

   protected RowIterator(Keyspace keyspace, String columnFamilyName, Serializer<K> keySerializer,
         Serializer<N> nameSerializer)
   {
      rangeSlicesQuery = HFactory.createRangeSlicesQuery(keyspace, keySerializer, nameSerializer,
            ByteBufferSerializer.get());

      rangeSlicesQuery.setColumnFamily(columnFamilyName);
      rangeSlicesQuery.setRange(null, null, false, 10);
      rangeSlicesQuery.setRowCount(ROW_COUNT);

      loadRowsPaged();
   }

   private void loadRowsPaged()
   {
      K keyOfLastRow = (row == null) ? null : row.getKey();

      rangeSlicesQuery.setKeys(keyOfLastRow, null);
      // System.out.println(" > " + keyOfLastRow);

      QueryResult<OrderedRows<K, N, ByteBuffer>> result = rangeSlicesQuery.execute();
      rows = result.get();
      rowsIterator = rows.iterator();

      // we'll skip this first one, since it is the same as the last one
      // from previous time we executed
      if (keyOfLastRow != null && rowsIterator != null)
      {
         rowsIterator.next();
      }
   }

   /**
    * Moves the iterator to the next row.
    * 
    * @return true if it now points to a row, else it returns false.
    */
   public boolean moveNext()
   {
      // check if we have read all the "paged" rows
      if (!rowsIterator.hasNext())
      {
         // if that was the last page, we are done, return false
         if (rows.getCount() < ROW_COUNT)
         {
            row = null;
            return false;
         }
         else
         {
            loadRowsPaged();
         }
      }
      row = rowsIterator.next();
      return true;
   }

   /**
    * Moves the iterator to the next row and skips empty column.
    * 
    * @return true if it now points to a row, else it returns false.
    */
   public boolean moveNextSkipEmptyRow()
   {
      while (moveNext())
      {
         if (!row.getColumnSlice().getColumns().isEmpty())
         {
            return true;
         }
      }
      return false;
   }

   /**
    * @return The key of the current row.
    */
   public K getKey()
   {
      return row.getKey();
   }

   /**
    * Gives a value of a "cell".
    * 
    * @param columnName The column name.
    * @return the value
    */
   public ByteBuffer getValueByColumnName(N columnName)
   {
      return row.getColumnSlice().getColumnByName(columnName).getValue();
   }
}