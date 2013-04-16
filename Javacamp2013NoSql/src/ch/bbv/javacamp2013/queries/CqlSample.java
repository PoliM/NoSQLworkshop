package ch.bbv.javacamp2013.queries;

import java.io.FileNotFoundException;
import java.io.IOException;

import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import ch.bbv.javacamp2013.Config;

public class CqlSample
{

   /**
    * @param args
    * @throws IOException
    * @throws FileNotFoundException
    */
   public static void main(String[] args) throws FileNotFoundException, IOException
   {
      Config cfg = new Config();
      Cluster cluster = HFactory.getOrCreateCluster(cfg.getClusterName(), cfg.getClusterAddress());

      Keyspace systemKeyspace = HFactory.createKeyspace("system", cluster);
      CqlQuery<String, String, String> cqlQuery = new CqlQuery<>(systemKeyspace, StringSerializer.get(),
            StringSerializer.get(), StringSerializer.get());

      cqlQuery.setQuery("select * from schema_keyspaces");
      // cqlQuery.setQuery("select * from local");
      QueryResult<CqlRows<String, String, String>> result = cqlQuery.execute();

      for (Row<String, String, String> row : result.get())
      {
         System.out.println(row);
      }
   }
}
