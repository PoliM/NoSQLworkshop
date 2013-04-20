package ch.bbv.javacamp2013.dao;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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
public class JavacampKeyspace {

   private static final String KEYSPACE_NAME = "javacamp";

   private final TweetDao tweetDao;

   private final UserDao userDao;

   private final UserlineDao userlineDao;

   private final WordSearchDao wordSearchDao;

   /**
    * Creates a new Javacamp keyspace.
    * 
    * @param clusterName Name of the cluster.
    * @param hostIp IP of the cluster initial node.
    */
   public JavacampKeyspace(final String clusterName, final String hostIp) {
      final Cluster myCluster = HFactory.getOrCreateCluster(clusterName, hostIp);

      setupKeyspaceDefinition(myCluster);

      final Keyspace ksp = HFactory.createKeyspace(KEYSPACE_NAME, myCluster);

      tweetDao = new TweetDao(ksp);
      userDao = new UserDao(ksp);
      userlineDao = new UserlineDao(ksp);
      wordSearchDao = new WordSearchDao(ksp);
   }

   public TweetDao getTweetDao() {
      return tweetDao;
   }

   public UserDao getUserDao() {
      return userDao;
   }

   public UserlineDao getUserlineDao() {
      return userlineDao;
   }

   public WordSearchDao getWordSearchDao() {
      return wordSearchDao;
   }

   private static void setupKeyspaceDefinition(final Cluster myCluster) {
      // Check if the keyspace is present, if not create it
      final KeyspaceDefinition keyspaceDef = myCluster.describeKeyspace(KEYSPACE_NAME);
      if (keyspaceDef == null) {
         createSchema(myCluster);
      }
      else if (!hasColumnFamily(keyspaceDef, WordSearchDao.getColumnFamilName())) {
         // WordSearch ColumnFamily was not included in first version
         addWordSearchColumnFamily(myCluster);
      }
   }

   private static KeyspaceDefinition createSchema(final Cluster myCluster) {
      // get the definition for every column family (table)
      final ColumnFamilyDefinition tweetDef = TweetDao.getColumnFamilyDefinition(KEYSPACE_NAME);

      final ColumnFamilyDefinition userDef = UserDao.getColumnFamilyDefinition(KEYSPACE_NAME);

      final ColumnFamilyDefinition userlineDef = UserlineDao.getColumnFamilyDefinition(KEYSPACE_NAME);

      final ColumnFamilyDefinition wordSearchDef = WordSearchDao.getColumnFamilyDefinition(KEYSPACE_NAME);

      // create the definition for the keyspace
      final KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(//
            KEYSPACE_NAME, // keyspaceName
            ThriftKsDef.DEF_STRATEGY_CLASS, // strategyClass
            3, // replicationFactor
            Arrays.asList(tweetDef, userDef, userlineDef, wordSearchDef)); // cfDefs

      // add the keyspace
      myCluster.addKeyspace(newKeyspace, true);

      return newKeyspace;
   }

   private static void addWordSearchColumnFamily(final Cluster myCluster) {
      final ColumnFamilyDefinition wordSearchDef = WordSearchDao.getColumnFamilyDefinition(KEYSPACE_NAME);

      myCluster.addColumnFamily(wordSearchDef, true);
   }

   private static boolean hasColumnFamily(final KeyspaceDefinition keyspaceDef, final String columnFamilName) {
      final List<ColumnFamilyDefinition> defs = keyspaceDef.getCfDefs();
      final Iterator<ColumnFamilyDefinition> iter = defs.iterator();
      while (iter.hasNext()) {
         final ColumnFamilyDefinition columnFamilyDefinition = iter.next();
         if (columnFamilyDefinition.getName().equals(columnFamilName)) {
            return true;
         }
      }
      return false;
   }
}
