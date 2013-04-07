package ch.bbv.javacamp2013;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import ch.bbv.javacamp2013.dao.JavacampKeyspace;
import ch.bbv.javacamp2013.twitter.TwitterStreamHandler;

public class StreamTwitterToCassandra
{

   private static final String CLUSTER_ADRESS = "192.168.20.130:9160";

   private static final String CLUSTER_NAME = "JavaCampNoSqlCluster";

   private static final List<Parameter> PARAMETER_SET = new LinkedList<Parameter>();

   public static void main(String[] args) throws FileNotFoundException, IOException
   {
      Properties props = readApplicationProperties();
      extractParametersFromProperties(props);

      JavacampKeyspace javacampKeyspace = new JavacampKeyspace(CLUSTER_NAME, CLUSTER_ADRESS);

      int paraId = 0;

      if (args.length > 0)
      {
         paraId = Integer.parseInt(args[0]);
      }

      Parameter para = PARAMETER_SET.get(paraId);
      System.out.println("Going to use " + para);

      TwitterStreamHandler twitterStreamHandler = new TwitterStreamHandler(javacampKeyspace, para.getUsername(),
            para.getPassword());

      twitterStreamHandler.stream(para.getFilter());
   }

   private static void extractParametersFromProperties(Properties props)
   {
      int userIdx = 1;
      while (props.containsKey(propKeyFor(userIdx, "name")))
      {
         String name = props.getProperty(propKeyFor(userIdx, "name"));
         String pw = props.getProperty(propKeyFor(userIdx, "pw"));
         String words = props.getProperty(propKeyFor(userIdx, "words"));
         String[] wordList = words.split("(,|\\s)+");
         PARAMETER_SET.add(new Parameter(name, pw, wordList));
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

   private static class Parameter
   {

      private final String _username;

      private final String _password;

      private final String[] _filter;

      public Parameter(String username, String password, String[] filter)
      {
         _username = username;
         _password = password;
         _filter = filter;
      }

      public String getUsername()
      {
         return _username;
      }

      public String getPassword()
      {
         return _password;
      }

      public String[] getFilter()
      {
         return _filter;
      }

      @Override
      public String toString()
      {
         StringBuffer str = new StringBuffer();
         str.append("Parameter(").append(_username).append(", [");
         boolean isFirst = true;
         for (String word : _filter)
         {
            if (isFirst)
            {
               isFirst = false;
            }
            else
            {
               str.append(", ");
            }
            str.append(word);
         }
         str.append("])");
         return str.toString();
      }
   }
}
