package ch.bbv.javacamp2013;

import java.io.IOException;

import ch.bbv.javacamp2013.dao.JavacampKeyspace;
import ch.bbv.javacamp2013.twitter.TwitterStreamHandler;

/**
 * Program that reads Tweets and stores them.
 */
public final class StreamTwitterToCassandra {

   private StreamTwitterToCassandra() {
   }

   /**
    * Start the program.
    * 
    * @param args Command line arguments.
    * @throws IOException If the configuration could not be read.
    */
   public static void main(final String[] args) throws IOException {
      final Config cfg = new Config();
      System.out.println("Connecting to cluster " + cfg.getClusterName() + " @ " + cfg.getClusterAddress());
      final JavacampKeyspace javacampKeyspace = new JavacampKeyspace(cfg.getClusterName(), cfg.getClusterAddress());

      int paraId = 0;

      if (args.length > 0) {
         paraId = Integer.parseInt(args[0]);
      }

      final ParameterData para = cfg.getParameterData(paraId);
      System.out.println("Going to use " + para);

      final TwitterStreamHandler twitterStreamHandler = new TwitterStreamHandler(javacampKeyspace, para.getUsername(),
            para.getPassword());

      twitterStreamHandler.stream(para.getFilter());
   }

}
