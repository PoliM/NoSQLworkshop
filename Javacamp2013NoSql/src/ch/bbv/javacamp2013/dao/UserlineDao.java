package ch.bbv.javacamp2013.dao;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
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
import ch.bbv.javacamp2013.model.Tweet;

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

   public List<Tweet> getUserline(long userId, TweetDao tweetDao)
   {
      ColumnFamilyResult<Long, Date> res = _template.queryColumns(userId);

      Collection<Date> columnNames = res.getColumnNames();
      List<Long> tweetIds = new ArrayList<>(columnNames.size());
      for (Date date : columnNames)
      {
         HColumn<Date, ByteBuffer> column = res.getColumn(date);
         tweetIds.add(LongSerializer.get().fromByteBuffer(column.getValue()));
      }

      return tweetDao.getTweets(tweetIds);
   }

   static ColumnFamilyDefinition getColumnFamilyDefinition(String keyspacename)
   {
      return HFactory.createColumnFamilyDefinition( // nl
            keyspacename, // keyspace
            COLUMNFAMILY_NAME, // cfName
            ComparatorType.DATETYPE); // comparatorType
   }
}