package ch.bbv.javacamp2013.queries;

import java.io.IOException;

import ch.bbv.javacamp2013.Config;
import ch.bbv.javacamp2013.dao.JavacampKeyspace;
import ch.bbv.javacamp2013.dao.TweetDao;
import ch.bbv.javacamp2013.dao.TweetDao.TweetIterator;

/**
 * Counts the tweets.
 */
public class CountTweets {

   private static final int REPORT_THRESHOLD = 10;

   /**
    * Starts to count all the tweets.
    * 
    * @param args Command line arguments.
    * @throws IOException If the configuration could not be read.
    */
   public static void main(String[] args) throws IOException {
      final Config cfg = new Config();
      System.out.println("Connecting to cluster " + cfg.getClusterName() + " @ " + cfg.getClusterAddress());
      final TweetDao tweetAccess = new JavacampKeyspace(cfg.getClusterName(), cfg.getClusterAddress()).getTweetDao();

      int count = 0;
      final TweetIterator i = tweetAccess.getIterator();
      while (i.moveNextSkipEmptyRow()) {
         if (count % REPORT_THRESHOLD == 0) {
            System.out.println(count + ": " + i.getKey() + ": userid=" + i.getUserId() + ", body=\"" + i.getBody()
                  + "\", createdAt=" + i.getCreatedAt());
         }
         count++;
      }
      System.out.println("Total=" + count);
   }

}
