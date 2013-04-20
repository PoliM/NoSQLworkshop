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
public class UserlineDao {
   private static final String COLUMNFAMILY_NAME = "Userline";

   private final ColumnFamilyTemplate<Long, Date> template;

   /**
    * Creates a new dao.
    * 
    * @param keyspace The keyspace.
    */
   UserlineDao(final Keyspace keyspace) {
      template = new ThriftColumnFamilyTemplate<Long, Date>(keyspace, // keyspace
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
   public void addUserlineEntry(final long userid, final Date createdAt, final long tweetid) {
      final ColumnFamilyUpdater<Long, Date> updater = template.createUpdater(userid);
      updater.setLong(createdAt, tweetid);

      try {
         template.update(updater);
      }
      catch (HectorException e) {
         e.printStackTrace();
      }
   }

   /**
    * Reads the tweets of a user.
    * 
    * @param userId The id of the user.
    * @param tweetDao The dao to access the users tweets.
    * @return A list of tweets of the user.
    */
   public List<Tweet> getUserline(final long userId, final TweetDao tweetDao) {
      final ColumnFamilyResult<Long, Date> res = template.queryColumns(userId);

      final Collection<Date> columnNames = res.getColumnNames();
      final List<Long> tweetIds = new ArrayList<>(columnNames.size());
      for (Date date : columnNames) {
         final HColumn<Date, ByteBuffer> column = res.getColumn(date);
         tweetIds.add(LongSerializer.get().fromByteBuffer(column.getValue()));
      }

      return tweetDao.getTweets(tweetIds);
   }

   /**
    * The column family of the userline.
    * 
    * @param keyspacename The name of the keyspace.
    * @return The CF of the userline.
    */
   static ColumnFamilyDefinition getColumnFamilyDefinition(final String keyspacename) {
      return HFactory.createColumnFamilyDefinition(//
            keyspacename, // keyspace
            COLUMNFAMILY_NAME, // cfName
            ComparatorType.DATETYPE); // comparatorType
   }
}