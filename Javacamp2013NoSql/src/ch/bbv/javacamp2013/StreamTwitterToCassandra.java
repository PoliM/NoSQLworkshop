package ch.bbv.javacamp2013;

import java.io.FileNotFoundException;
import java.io.IOException;

import ch.bbv.javacamp2013.dao.JavacampKeyspace;
import ch.bbv.javacamp2013.twitter.TwitterStreamHandler;

public class StreamTwitterToCassandra
{
   public static void main(String[] args) throws FileNotFoundException, IOException
   {
      Config cfg = new Config();
      System.out.println("Connecting to cluster " + cfg.getClusterName() + " @ " + cfg.getClusterAddress());
      JavacampKeyspace javacampKeyspace = new JavacampKeyspace(cfg.getClusterName(), cfg.getClusterAddress());

      int paraId = 0;

      if (args.length > 0)
      {
         paraId = Integer.parseInt(args[0]);
      }

      ParameterData para = cfg.getParameterData(paraId);
      System.out.println("Going to use " + para);

      TwitterStreamHandler twitterStreamHandler = new TwitterStreamHandler(javacampKeyspace, para.getUsername(),
            para.getPassword());

      twitterStreamHandler.stream(para.getFilter());
   }

}
