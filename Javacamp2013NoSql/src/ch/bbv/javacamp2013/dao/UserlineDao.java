package ch.bbv.javacamp2013.dao;

import java.util.Date;

import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * Implements high level methods to access the "Userline" column family (table).<br>
 * To get an instance of this object call
 * {@link JavacampKeyspace#getUserlineDao}.<br>
 * The table has the following layout:
 * <ul>
 * <li>The key for the row is the userid.</li>
 * <li>The key for the col is time when the tweet was created.</li>
 * <li>The value of the cell is the id of the tweet.</li>
 * </ul>
 */
public class UserlineDao
{

   private static final String COLUMNFAMILY_NAME = "Userline";

   private final ColumnFamilyTemplate<Long, Date> _template;

   UserlineDao(Keyspace keyspace)
   {
      _template = new ThriftColumnFamilyTemplate<Long, Date>(keyspace, // keyspace
            COLUMNFAMILY_NAME, // columnFamily
            LongSerializer.get(), // keySerializer
            DateSerializer.get()); // topSerializer
   }

   /**
    * Adds an entry to the Userline column family.
    * 
    * @param userid The is of the user
    * @param createdAt The tiem when the tweet was created
    * @param tweetid The id of the tweet.
    */
   public void addUserlineEntry(long userid, Date createdAt, long tweetid)
   {
      ColumnFamilyUpdater<Long, Date> updater = _template.createUpdater(userid);
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
}