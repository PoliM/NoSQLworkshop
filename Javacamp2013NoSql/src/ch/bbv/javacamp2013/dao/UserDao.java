package ch.bbv.javacamp2013.dao;

import java.util.Arrays;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
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

   UserDao(Keyspace keyspace)
   {
      _template = new ThriftColumnFamilyTemplate<Long, String>( // nl
            keyspace, // keyspace
            COLUMNFAMILY_NAME, // columnFamily
            LongSerializer.get(), // keySerializer
            StringSerializer.get()); // topSerializer
   }

   /**
    * Adds an user to the column family (table).
    * 
    * @param userid The id of the user.
    * @param name The name of the user.
    * @param screenName The screen name of the user.
    */
   public void addUser(long userid, String name, String screenName)
   {
      ColumnFamilyUpdater<Long, String> updater = _template.createUpdater(userid);
      updater.setLong(COL_USER_ID, userid);
      updater.setString(COL_NAME, name);
      updater.setString(COL_SCREEN_NAME, screenName);

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
      idColDef.setName(StringSerializer.get().toByteBuffer(COL_USER_ID));
      idColDef.setIndexName(COL_USER_ID + "_idx");
      idColDef.setIndexType(ColumnIndexType.KEYS);
      idColDef.setValidationClass(ComparatorType.LONGTYPE.getClassName());

      BasicColumnDefinition nameColDef = new BasicColumnDefinition();
      nameColDef.setName(StringSerializer.get().toByteBuffer(COL_NAME));
      nameColDef.setValidationClass(ComparatorType.UTF8TYPE.getClassName());

      BasicColumnDefinition screenNameColDef = new BasicColumnDefinition();
      screenNameColDef.setName(StringSerializer.get().toByteBuffer(COL_SCREEN_NAME));
      screenNameColDef.setValidationClass(ComparatorType.UTF8TYPE.getClassName());

      return HFactory.createColumnFamilyDefinition( // nl
            keyspacename, // keinyspace
            COLUMNFAMILY_NAME, // cfName
            ComparatorType.UTF8TYPE, // comparatorType
            Arrays.asList((ColumnDefinition) idColDef, (ColumnDefinition) nameColDef, (ColumnDefinition) nameColDef,
                  (ColumnDefinition) screenNameColDef));

   }
}