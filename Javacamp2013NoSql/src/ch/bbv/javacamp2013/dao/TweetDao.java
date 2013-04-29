package ch.bbv.javacamp2013.dao;

import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import ch.bbv.javacamp2013.model.Tweet;

/**
 * Implements high level methods to access the "Tweets" column family (table).
 * To get an instance of this object call {@link JavacampKeyspace#getTweetDao}.<br>
 * The table has the following layout:
 * <ul>
 * <li>The key for the row is the tweetid.</li>
 * <li>The col <code>user_id</code> contains the id of the user, that created
 * the tweet.</li>
 * <li>The col <code>body</code> contains the message of the tweet.</li>
 * <li>The col <code>created_at</code> contains the time when the tweet was
 * created.</li>
 * </ul>
 */
public class TweetDao {

   private static final String COLUMNFAMILY_NAME = "Tweets";

   private static final String COL_TWEET_ID = "id";

   private static final String COL_USER_ID = "user_id";

   private static final String COL_BODY = "body";

   private static final String COL_CREATED_AT = "created_at";

   private final Keyspace keyspace;

   private final ColumnFamilyTemplate<Long, String> template;

   /**
    * Creates a new dao.
    * 
    * @param keyspace The keyspace.
    */
   TweetDao(final Keyspace keyspace) {
      this.keyspace = keyspace;

      template = new ThriftColumnFamilyTemplate<Long, String>(keyspace, // keyspace
            COLUMNFAMILY_NAME, // columnFamily
            getKeySerializer(), // keySerializer
            getColumnNameSerializer()); // topSerializer
   }

   /**
    * Adds an tweet to the column family (table).
    * 
    * @param tweet The tweet.
    */
   public final void addTweet(final Tweet tweet) {
      final ColumnFamilyUpdater<Long, String> updater = template.createUpdater(tweet.getTweetid());
      updater.setLong(COL_TWEET_ID, tweet.getTweetid());
      updater.setLong(COL_USER_ID, tweet.getUserid());
      updater.setString(COL_BODY, tweet.getBody());
      updater.setDate(COL_CREATED_AT, tweet.getCreatedAt());

      try {
         template.update(updater);
      }
      catch (HectorException e) {
         e.printStackTrace();
      }
   }

   /**
    * The column definition for tweets.
    * 
    * @param keyspacename Name of the keyspace.
    * @return The colum definition
    */
   static ColumnFamilyDefinition getColumnFamilyDefinition(final String keyspacename) {
      final BasicColumnDefinition idColDef = new BasicColumnDefinition();
      idColDef.setName(getColumnNameSerializer().toByteBuffer(COL_TWEET_ID));
      idColDef.setIndexName(COL_TWEET_ID + "_idx");
      idColDef.setIndexType(ColumnIndexType.KEYS);
      idColDef.setValidationClass(ComparatorType.LONGTYPE.getClassName());

      final BasicColumnDefinition userColDef = new BasicColumnDefinition();
      userColDef.setName(getColumnNameSerializer().toByteBuffer(COL_USER_ID));
      userColDef.setValidationClass(ComparatorType.LONGTYPE.getClassName());

      final BasicColumnDefinition bodyColDef = new BasicColumnDefinition();
      bodyColDef.setName(getColumnNameSerializer().toByteBuffer(COL_BODY));
      bodyColDef.setValidationClass(ComparatorType.UTF8TYPE.getClassName());

      final BasicColumnDefinition createdAtColDef = new BasicColumnDefinition();
      createdAtColDef.setName(getColumnNameSerializer().toByteBuffer(COL_CREATED_AT));
      createdAtColDef.setValidationClass(ComparatorType.DATETYPE.getClassName());

      return HFactory.createColumnFamilyDefinition(//
            keyspacename, // keinyspace
            COLUMNFAMILY_NAME, // cfName
            ComparatorType.UTF8TYPE, // comparatorType
            Arrays.asList((ColumnDefinition) idColDef, (ColumnDefinition) userColDef, (ColumnDefinition) bodyColDef,
                  (ColumnDefinition) createdAtColDef));

   }

   /**
    * Reads a tweet.
    * 
    * @param tweetid Id of the tweet
    * @return The tweet.
    */
   public final Tweet getTweet(final long tweetid) {
      final ColumnFamilyResult<Long, String> res = template.queryColumns(tweetid);

      return getTweet(res);
   }

   private static LongSerializer getKeySerializer() {
      return LongSerializer.get();
   }

   private static StringSerializer getColumnNameSerializer() {
      return StringSerializer.get();
   }

   private Tweet getTweet(final ColumnFamilyResult<Long, String> res) {
      final long tweetId = res.getLong(COL_TWEET_ID);
      final long userId = res.getLong(COL_USER_ID);
      final String body = res.getString(COL_BODY);
      final Date createdAt = res.getDate(COL_CREATED_AT);

      return new Tweet(tweetId, userId, body, createdAt);
   }

   /**
    * Reads a list of tweets.
    * 
    * @param tweetIds All the tweets to read.
    * @return The list with the tweets for each id.
    */
   public final List<Tweet> getTweets(final List<Long> tweetIds) {
      final List<Tweet> tweets = new LinkedList<>();
      final ColumnFamilyResult<Long, String> res = template.queryColumns(tweetIds);

      while (res.hasNext()) {
         tweets.add(getTweet(res));
         res.next();
      }
      return tweets;
   }

   /**
    * Returns a Iterator to get all the tweets.
    * 
    * @return The iterator.
    */
   public TweetIterator getIterator() {
      return new TweetIterator(keyspace);
   }

   /**
    * Iterator for tweets.
    */
   public static class TweetIterator extends RowIterator<Long, String> {

      /**
       * Creates a new iterator.
       * 
       * @param keyspace The keyspace.
       */
      public TweetIterator(final Keyspace keyspace) {
         super(keyspace, COLUMNFAMILY_NAME, getKeySerializer(), getColumnNameSerializer());
      }

      public long getUserId() {
         return LongSerializer.get().fromByteBuffer(getValueByColumnName(COL_USER_ID));
      }

      public Date getCreatedAt() {
         return DateSerializer.get().fromByteBuffer(getValueByColumnName(COL_CREATED_AT));
      }

      public String getBody() {
         return StringSerializer.get().fromByteBuffer(getValueByColumnName(COL_BODY));
      }
   }
}
