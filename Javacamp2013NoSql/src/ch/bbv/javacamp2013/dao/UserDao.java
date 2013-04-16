package ch.bbv.javacamp2013.dao;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
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
import ch.bbv.javacamp2013.model.User;

/**
 * Implements high level methods to access the "Users" column family (table). To
 * get an instance of this object call {@link JavacampKeyspace#getUserDao}.<br>
 * The table has the following layout:
 * <ul>
 * <li>The key for the row is the userid.</li>
 * <li>The col <code>name</code> contains the name of the user.</li>
 * <li>The col <code>screen_name</code> contains the screen name of the user.</li>
 * </ul>
 */
public class UserDao
{
   private static final String COLUMNFAMILY_NAME = "Users";

   private static final String COL_USER_ID = "id";

   private static final String COL_NAME = "name";

   private static final String COL_SCREEN_NAME = "screen_name";

   private final ColumnFamilyTemplate<Long, String> _template;

   private final Keyspace _keyspace;

   private static LongSerializer getKeySerializer()
   {
      return LongSerializer.get();
   }

   private static StringSerializer getColumnNameSerializer()
   {
      return StringSerializer.get();
   }

   UserDao(Keyspace keyspace)
   {
      _keyspace = keyspace;

      _template = new ThriftColumnFamilyTemplate<Long, String>( // nl
            keyspace, // keyspace
            COLUMNFAMILY_NAME, // columnFamily
            getKeySerializer(), // keySerializer
            getColumnNameSerializer()); // topSerializer
   }

   /**
    * Adds an user to the column family (table).
    * 
    * @param userid The id of the user.
    * @param name The name of the user.
    * @param screenName The screen name of the user.
    */
   public void addUser(User user)
   {
      ColumnFamilyUpdater<Long, String> updater = _template.createUpdater(user.getUserid());
      updater.setLong(COL_USER_ID, user.getUserid());
      updater.setString(COL_NAME, user.getName());
      updater.setString(COL_SCREEN_NAME, user.getScreenName());

      try
      {
         _template.update(updater);
      }
      catch (HectorException e)
      {
         e.printStackTrace();
      }
   }

   public User getUser(long userId)
   {
      ColumnFamilyResult<Long, String> res = _template.queryColumns(userId);

      String name = res.getString(COL_NAME);
      String screenName = res.getString(COL_SCREEN_NAME);

      return new User(userId, name, screenName);
   }

   static ColumnFamilyDefinition getColumnFamilyDefinition(String keyspacename)
   {
      BasicColumnDefinition idColDef = new BasicColumnDefinition();
      idColDef.setName(getColumnNameSerializer().toByteBuffer(COL_USER_ID));
      idColDef.setIndexName(COL_USER_ID + "_idx");
      idColDef.setIndexType(ColumnIndexType.KEYS);
      idColDef.setValidationClass(ComparatorType.LONGTYPE.getClassName());

      BasicColumnDefinition nameColDef = new BasicColumnDefinition();
      nameColDef.setName(getColumnNameSerializer().toByteBuffer(COL_NAME));
      nameColDef.setValidationClass(ComparatorType.UTF8TYPE.getClassName());

      BasicColumnDefinition screenNameColDef = new BasicColumnDefinition();
      screenNameColDef.setName(getColumnNameSerializer().toByteBuffer(COL_SCREEN_NAME));
      screenNameColDef.setValidationClass(ComparatorType.UTF8TYPE.getClassName());

      return HFactory.createColumnFamilyDefinition( // nl
            keyspacename, // keinyspace
            COLUMNFAMILY_NAME, // cfName
            ComparatorType.UTF8TYPE, // comparatorType
            Arrays.asList((ColumnDefinition) idColDef, (ColumnDefinition) nameColDef, (ColumnDefinition) nameColDef,
                  (ColumnDefinition) screenNameColDef));

   }

   /**
    * Returns a Iterator to get all the users.
    * 
    * @return The iterator.
    */
   public UserIterator getIterator()
   {
      return new UserIterator(_keyspace);
   }

   public static class UserIterator extends RowIterator<Long, String>
   {
      public UserIterator(Keyspace keyspace)
      {
         super(keyspace, COLUMNFAMILY_NAME, getKeySerializer(), getColumnNameSerializer());
      }

      public long getUserId()
      {
         return LongSerializer.get().fromByteBuffer(getValueByColumnName(COL_USER_ID));
      }

      public String getName()
      {
         return StringSerializer.get().fromByteBuffer(getValueByColumnName(COL_NAME));
      }

      public String getScreenName()
      {
         return StringSerializer.get().fromByteBuffer(getValueByColumnName(COL_SCREEN_NAME));
      }
   }

   public static void main(String[] args) throws FileNotFoundException, IOException
   {
      Config cfg = new Config();
      System.out.println("Connecting to cluster " + cfg.getClusterName() + " @ " + cfg.getClusterAddress());
      UserDao userAccess = new JavacampKeyspace(cfg.getClusterName(), cfg.getClusterAddress()).getUserDao();

      int count = 0;
      UserIterator i = userAccess.getIterator();
      while (i.moveNextSkipEmptyRow())
      {
         if (count % 10000 == 0)
         {
            System.out.println(count + ": " + i.getKey() + ": userid=" + i.getUserId() + ", name=\"" + i.getName()
                  + "\", screenName=" + i.getScreenName());
         }
         count++;
      }
      System.out.println("Total=" + count);
   }
}