package ch.bbv.javacamp2013;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Encapsulates the configuration. Also reads the configuration form the config
 * file.
 */
public class Config {
   private static final String PROP_NAME = "name";

   private static final String PROP_CLUSTER_ADDRESS = "cluster.address";

   private static final String CLUSTER_NAME = "JavaCampNoSqlCluster";

   private String clusterAddress = "192.168.20.130:9160";

   private final List<ParameterData> parameterSet = new LinkedList<ParameterData>();

   /**
    * Reads the configuration.
    * 
    * @throws IOException If the configuration could not be read.
    */
   public Config() throws IOException {
      final Properties props = readApplicationProperties();
      extractParametersFromProperties(props);
      extractClusterFromProperties(props);
   }

   public String getClusterAddress() {
      return clusterAddress;
   }

   public String getClusterName() {
      return CLUSTER_NAME;
   }

   /**
    * Gets the nth. parameter.
    * 
    * @param idx Index of the parameter
    * @return The nth. parameter.
    */
   public ParameterData getParameterData(final int idx) {
      return parameterSet.get(idx);
   }

   private void extractClusterFromProperties(final Properties props) {
      if (props.containsKey(PROP_CLUSTER_ADDRESS)) {
         clusterAddress = props.getProperty(PROP_CLUSTER_ADDRESS);
      }
   }

   private void extractParametersFromProperties(final Properties props) {
      int userIdx = 1;
      while (props.containsKey(propKeyFor(userIdx, PROP_NAME))) {
         final String name = props.getProperty(propKeyFor(userIdx, PROP_NAME));
         final String passw = props.getProperty(propKeyFor(userIdx, "pw"));
         final String words = props.getProperty(propKeyFor(userIdx, "words"));
         final String[] wordList = words.split("(,|\\s)+");
         parameterSet.add(new ParameterData(name, passw, wordList));
         userIdx++;
      }
   }

   private static Properties readApplicationProperties() throws IOException {
      final File propsFile = new File(System.getProperty("user.home"), ".sttc.properties");
      if (!propsFile.exists()) {
         System.err.println("Please create the configuration file: " + propsFile.getAbsolutePath());
         System.exit(1);
      }

      final Properties props = new Properties();
      props.load(new FileReader(propsFile));
      return props;
   }

   private static String propKeyFor(final int userIdx, final String key) {
      return "user" + userIdx + "." + key;
   }
}
