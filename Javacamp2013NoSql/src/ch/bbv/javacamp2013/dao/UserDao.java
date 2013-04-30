package ch.bbv.javacamp2013.dao;

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
public class UserDao {

   private static final String COLUMNFAMILY_NAME = "Users";

   private static final String COL_USER_ID = "id";

   private static final String COL_NAME = "name";

   private static final String COL_SCREEN_NAME = "screen_name";

   private final ColumnFamilyTemplate<Long, String> template;

   private final Keyspace keyspace;

   /**
    * Creates a new dao.
    * 
    * @param keyspace The keyspace.
    */
   UserDao(final Keyspace keyspace) {
      this.keyspace = keyspace;

      template = new ThriftColumnFamilyTemplate<Long, String>(//
            keyspace, // keyspace
            COLUMNFAMILY_NAME, // columnFamily
            getKeySerializer(), // keySerializer
            getColumnNameSerializer()); // topSerializer
   }

   /**
    * Adds an user to the column family (table).
    * 
    * @param user The user data.
    */
   public void addUser(final User user) {
      final ColumnFamilyUpdater<Long, String> updater = template.createUpdater(user.getUserid());
      updater.setLong(COL_USER_ID, user.getUserid());
      updater.setString(COL_NAME, user.getName());
      updater.setString(COL_SCREEN_NAME, user.getScreenName());

      try {
         template.update(updater);
      }
      catch (HectorException e) {
         e.printStackTrace();
      }
   }

   /**
    * Reads a user.
    * 
    * @param userId The id of the user.
    * @return The user.
    */
   public User getUser(final long userId) {
      final ColumnFamilyResult<Long, String> res = template.queryColumns(userId);

      final String name = res.getString(COL_NAME);
      final String screenName = res.getString(COL_SCREEN_NAME);

      return new User(userId, name, screenName);
   }

   /**
    * The column definition of user.
    * 
    * @param keyspacename The name of the keyspace.
    * @return The definition of the column family.
    */
   static ColumnFamilyDefinition getColumnFamilyDefinition(final String keyspacename) {
      final BasicColumnDefinition idColDef = new BasicColumnDefinition();
      idColDef.setName(getColumnNameSerializer().toByteBuffer(COL_USER_ID));
      idColDef.setIndexName(COL_USER_ID + "_idx");
      idColDef.setIndexType(ColumnIndexType.KEYS);
      idColDef.setValidationClass(ComparatorType.LONGTYPE.getClassName());

      final BasicColumnDefinition nameColDef = new BasicColumnDefinition();
      nameColDef.setName(getColumnNameSerializer().toByteBuffer(COL_NAME));
      nameColDef.setValidationClass(ComparatorType.UTF8TYPE.getClassName());

      final BasicColumnDefinition screenNameColDef = new BasicColumnDefinition();
      screenNameColDef.setName(getColumnNameSerializer().toByteBuffer(COL_SCREEN_NAME));
      screenNameColDef.setValidationClass(ComparatorType.UTF8TYPE.getClassName());

      return HFactory.createColumnFamilyDefinition(//
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
   public UserIterator getIterator() {
      return new UserIterator(keyspace);
   }

   private static LongSerializer getKeySerializer() {
      return LongSerializer.get();
   }

   private static StringSerializer getColumnNameSerializer() {
      return StringSerializer.get();
   }

   /**
    * The iterator for user.
    */
   public static class UserIterator extends RowIterator<Long, String> {

      /**
       * Creates a new iterator.
       * 
       * @param keyspace The keyspace.
       */
      public UserIterator(final Keyspace keyspace) {
         super(keyspace, COLUMNFAMILY_NAME, getKeySerializer(), getColumnNameSerializer());
      }

      public long getUserId() {
         return LongSerializer.get().fromByteBuffer(getValueByColumnName(COL_USER_ID));
      }

      public String getName() {
         return StringSerializer.get().fromByteBuffer(getValueByColumnName(COL_NAME));
      }

      public String getScreenName() {
         return StringSerializer.get().fromByteBuffer(getValueByColumnName(COL_SCREEN_NAME));
      }
   }

}