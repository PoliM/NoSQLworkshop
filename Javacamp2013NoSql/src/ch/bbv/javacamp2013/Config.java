package ch.bbv.javacamp2013;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class Config
{
   private String clusterAddress = "192.168.20.130:9160";

   private final String clusterName = "JavaCampNoSqlCluster";

   private final List<ParameterData> parameterSet = new LinkedList<ParameterData>();

   public Config() throws FileNotFoundException, IOException
   {
      Properties props = readApplicationProperties();
      extractParametersFromProperties(props);
      extractClusterFromProperties(props);
   }

   public String getClusterAddress()
   {
      return clusterAddress;
   }

   public String getClusterName()
   {
      return clusterName;
   }

   public ParameterData getParameterData(int idx)
   {
      return parameterSet.get(idx);
   }

   private void extractClusterFromProperties(Properties props)
   {
      if (props.containsKey("cluster.address"))
      {
         clusterAddress = props.getProperty("cluster.address");
      }
   }

   private void extractParametersFromProperties(Properties props)
   {
      int userIdx = 1;
      while (props.containsKey(propKeyFor(userIdx, "name")))
      {
         String name = props.getProperty(propKeyFor(userIdx, "name"));
         String pw = props.getProperty(propKeyFor(userIdx, "pw"));
         String words = props.getProperty(propKeyFor(userIdx, "words"));
         String[] wordList = words.split("(,|\\s)+");
         parameterSet.add(new ParameterData(name, pw, wordList));
         userIdx++;
      }
   }

   private static Properties readApplicationProperties() throws IOException, FileNotFoundException
   {
      File propsFile = new File(System.getProperty("user.home"), ".sttc.properties");
      if (!propsFile.exists())
      {
         System.err.println("Please create the configuration file: " + propsFile.getAbsolutePath());
         System.exit(1);
      }

      Properties props = new Properties();
      props.load(new FileReader(propsFile));
      return props;
   }

   private static String propKeyFor(int userIdx, String key)
   {
      return "user" + userIdx + "." + key;
   }
}
