package ch.bbv.javacamp2013.dao;

import java.util.Date;

import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
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

   WordSearchDao(Keyspace keyspace)
   {
      _template = new ThriftColumnFamilyTemplate<String, Date>(keyspace, // keyspace
            COLUMNFAMILY_NAME, // columnFamily
            StringSerializer.get(), // keySerializer
            DateSerializer.get()); // topSerializer
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
}