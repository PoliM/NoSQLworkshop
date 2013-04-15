package ch.bbv.javacamp2013.dao;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.TreeMap;

import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * Implements high level methods to access the "WordSearch" column family
 * (table).<br>
 * To get an instance of this object call
 * {@link JavacampKeyspace#getWordSearchDao}.<br>
 * The table has the following layout:
 * <ul>
 * <li>The key for the row is the word.</li>
 * <li>The key for the col is time when the tweet was created.</li>
 * <li>The value of the cell is the id of the tweet.</li>
 * </ul>
 */
public class WordSearchDao
{

   private static final String COLUMNFAMILY_NAME = "WordSearch";

   private final ColumnFamilyTemplate<String, Date> _template;

   private final Keyspace _keyspace;

   private static StringSerializer getKeySerializer()
   {
      return StringSerializer.get();
   }

   private static DateSerializer getColumnNameSerializer()
   {
      return DateSerializer.get();
   }

   WordSearchDao(Keyspace keyspace)
   {
      _keyspace = keyspace;

      _template = new ThriftColumnFamilyTemplate<String, Date>(keyspace, // keyspace
            COLUMNFAMILY_NAME, // columnFamily
            getKeySerializer(), // keySerializer
            getColumnNameSerializer()); // topSerializer
   }

   /**
    * Adds an entry to the WordSearch column family.
    * 
    * @param word The word
    * @param createdAt The time when the tweet was created
    * @param tweetid The id of the tweet.
    */
   public void addWordEntry(String word, Date createdAt, long tweetid)
   {
      ColumnFamilyUpdater<String, Date> updater = _template.createUpdater(word);
      updater.setLong(createdAt, tweetid);

      try
      {
         _template.update(updater);
      }
      catch (HectorException e)
      {
         e.printStackTrace();
      }
   }

   public TreeMap<Date, Long> getTweetIdsForWord(String word)
   {
      TreeMap<Date, Long> result = new TreeMap<>();
      try
      {
         ColumnFamilyResult<String, Date> res = _template.queryColumns(word);

         for (Date date : res.getColumnNames())
         {
            HColumn<Date, ByteBuffer> column = res.getColumn(date);
            result.put(date, LongSerializer.get().fromByteBuffer(column.getValue()));
         }
      }
      catch (HectorException e)
      {
         e.printStackTrace();
      }
      return result;
   }

   static ColumnFamilyDefinition getColumnFamilyDefinition(String keyspacename)
   {
      return HFactory.createColumnFamilyDefinition( // nl
            keyspacename, // keyspace
            COLUMNFAMILY_NAME, // cfName
            ComparatorType.DATETYPE); // comparatorType
   }

   static String getColumnFamilName()
   {
      return COLUMNFAMILY_NAME;
   }

   /**
    * Returns a Iterator to get all the tweets.
    * 
    * @return The iterator.
    */
   public WordIterator getIterator()
   {
      return new WordIterator(_keyspace);
   }

   public static class WordIterator extends RowIterator<String, Date>
   {
      public WordIterator(Keyspace keyspace)
      {
         super(keyspace, COLUMNFAMILY_NAME, getKeySerializer(), getColumnNameSerializer());
      }
   }
}