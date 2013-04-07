package ch.bbv.javacamp2013.dao;

import java.util.Arrays;

import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * The main class for accessing cassandra. It returns instances of the different
 * DAOs to access cassandra.
 */
public class JavacampKeyspace
{

   private static final String KEYSPACE_NAME = "javacamp";

   private final TweetDao _tweetDao;

   private final UserDao _userDao;

   private final UserlineDao _userlineDao;

   public JavacampKeyspace(String clusterName, String hostIp)
   {
      Cluster myCluster = HFactory.getOrCreateCluster(clusterName, hostIp);

      // Check if the keyspace is present, if not create it
      KeyspaceDefinition keyspaceDef = myCluster.describeKeyspace(KEYSPACE_NAME);
      if (keyspaceDef == null)
      {
         createSchema(myCluster);
      }

      Keyspace ksp = HFactory.createKeyspace(KEYSPACE_NAME, myCluster);

      _tweetDao = new TweetDao(ksp);
      _userDao = new UserDao(ksp);
      _userlineDao = new UserlineDao(ksp);
   }

   public TweetDao getTweetDao()
   {
      return _tweetDao;
   }

   public UserDao getUserDao()
   {
      return _userDao;
   }

   public UserlineDao getUserlineDao()
   {
      return _userlineDao;
   }

   private static KeyspaceDefinition createSchema(Cluster myCluster)
   {
      // get the definition for every column family (table)
      ColumnFamilyDefinition tweetDef = TweetDao.getColumnFamilyDefinition(KEYSPACE_NAME);

      ColumnFamilyDefinition userDef = UserDao.getColumnFamilyDefinition(KEYSPACE_NAME);

      ColumnFamilyDefinition userlineDef = UserlineDao.getColumnFamilyDefinition(KEYSPACE_NAME);

      // create the definition for the keyspace
      KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition( // nl
            KEYSPACE_NAME, // keyspaceName
            ThriftKsDef.DEF_STRATEGY_CLASS, // strategyClass
            3, // replicationFactor
            Arrays.asList(tweetDef, userDef, userlineDef)); // cfDefs

      // add the keyspace
      myCluster.addKeyspace(newKeyspace, true);

      return newKeyspace;
   }
}
