package ch.bbv.javacamp2013.dao;

import java.io.FileNotFoundException;
import java.io.IOException;
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
import ch.bbv.javacamp2013.Config;
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
public class TweetDao
{
   private static final String COLUMNFAMILY_NAME = "Tweets";

   private static final String COL_TWEET_ID = "id";

   private static final String COL_USER_ID = "user_id";

   private static final String COL_BODY = "body";

   private static final String COL_CREATED_AT = "created_at";

   private final Keyspace _keyspace;

   private final ColumnFamilyTemplate<Long, String> _template;

   private static LongSerializer getKeySerializer()
   {
      return LongSerializer.get();
   }

   private static StringSerializer getColumnNameSerializer()
   {
      return StringSerializer.get();
   }

   TweetDao(Keyspace keyspace)
   {
      _keyspace = keyspace;

      _template = new ThriftColumnFamilyTemplate<Long, String>(_keyspace, // keyspace
            COLUMNFAMILY_NAME, // columnFamily
            getKeySerializer(), // keySerializer
            getColumnNameSerializer()); // topSerializer
   }

   /**
    * Adds an tweet to the column family (table)
    * 
    * @param tweetid The id of the tweet.
    * @param userid The id of the user, that created the tweet.
    * @param body The message of the tweet.
    * @param createdAt The time when the tweet was created.
    */
   public void addTweet(Tweet tweet)
   {
      ColumnFamilyUpdater<Long, String> updater = _template.createUpdater(tweet.getTweetid());
      updater.setLong(COL_TWEET_ID, tweet.getTweetid());
      updater.setLong(COL_USER_ID, tweet.getUserid());
      updater.setString(COL_BODY, tweet.getBody());
      updater.setDate(COL_CREATED_AT, tweet.getCreatedAt());

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
      BasicColumnDefinition idColDef = new BasicColumnDefinition();
      idColDef.setName(getColumnNameSerializer().toByteBuffer(COL_TWEET_ID));
      idColDef.setIndexName(COL_TWEET_ID + "_idx");
      idColDef.setIndexType(ColumnIndexType.KEYS);
      idColDef.setValidationClass(ComparatorType.LONGTYPE.getClassName());

      BasicColumnDefinition userColDef = new BasicColumnDefinition();
      userColDef.setName(getColumnNameSerializer().toByteBuffer(COL_USER_ID));
      userColDef.setValidationClass(ComparatorType.LONGTYPE.getClassName());

      BasicColumnDefinition bodyColDef = new BasicColumnDefinition();
      bodyColDef.setName(getColumnNameSerializer().toByteBuffer(COL_BODY));
      bodyColDef.setValidationClass(ComparatorType.UTF8TYPE.getClassName());

      BasicColumnDefinition createdAtColDef = new BasicColumnDefinition();
      createdAtColDef.setName(getColumnNameSerializer().toByteBuffer(COL_CREATED_AT));
      createdAtColDef.setValidationClass(ComparatorType.DATETYPE.getClassName());

      return HFactory.createColumnFamilyDefinition( // nl
            keyspacename, // keinyspace
            COLUMNFAMILY_NAME, // cfName
            ComparatorType.UTF8TYPE,// comparatorType
            Arrays.asList((ColumnDefinition) idColDef, (ColumnDefinition) userColDef, (ColumnDefinition) bodyColDef,
                  (ColumnDefinition) createdAtColDef));

   }

   public Tweet getTweet(long tweetid)
   {
      ColumnFamilyResult<Long, String> res = _template.queryColumns(tweetid);

      return getTweet(res);
   }

   private Tweet getTweet(ColumnFamilyResult<Long, String> res)
   {
      long tweetId = res.getLong(COL_TWEET_ID);
      long userId = res.getLong(COL_USER_ID);
      String body = res.getString(COL_BODY);
      Date createdAt = res.getDate(COL_CREATED_AT);

      return new Tweet(tweetId, userId, body, createdAt);
   }

   public List<Tweet> getTweets(List<Long> tweetIds)
   {
      List<Tweet> tweets = new LinkedList<>();
      ColumnFamilyResult<Long, String> res = _template.queryColumns(tweetIds);

      while (res.hasNext())
      {
         tweets.add(getTweet(res));
         res.next();
      }
      return tweets;
   }

   public static void main(String[] args) throws FileNotFoundException, IOException
   {
      Config cfg = new Config();
      System.out.println("Connecting to cluster " + cfg.getClusterName() + " @ " + cfg.getClusterAddress());
      TweetDao tweetAccess = new JavacampKeyspace(cfg.getClusterName(), cfg.getClusterAddress()).getTweetDao();

      int count = 0;
      TweetIterator i = tweetAccess.getIterator();
      while (i.moveNextSkipEmptyRow())
      {
         if (count % 10000 == 0)
         {
            System.out.println(count + ": " + i.getKey() + ": userid=" + i.getUserId() + ", body=\"" + i.getBody()
                  + "\", createdAt=" + i.getCreatedAt());
         }
         count++;
      }
      System.out.println("Total=" + count);

      // long id = 1234;
      // tweetAccess.addTweet(id, 23456, "My body", new Date());
      // tweetAccess.getTweet(id);
      // tweetAccess.getTweet(id);
   }

   /**
    * Returns a Iterator to get all the tweets.
    * 
    * @return The iterator.
    */
   public TweetIterator getIterator()
   {
      return new TweetIterator(_keyspace);
   }

   public static class TweetIterator extends RowIterator<Long, String>
   {
      public TweetIterator(Keyspace keyspace)
      {
         super(keyspace, COLUMNFAMILY_NAME, getKeySerializer(), getColumnNameSerializer());
      }

      public long getUserId()
      {
         return LongSerializer.get().fromByteBuffer(getValueByColumnName(COL_USER_ID));
      }

      public Date getCreatedAt()
      {
         return DateSerializer.get().fromByteBuffer(getValueByColumnName(COL_CREATED_AT));
      }

      public String getBody()
      {
         return StringSerializer.get().fromByteBuffer(getValueByColumnName(COL_BODY));
      }
   }
}
