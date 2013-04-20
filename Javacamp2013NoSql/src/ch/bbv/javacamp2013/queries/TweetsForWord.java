package ch.bbv.javacamp2013.queries;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import ch.bbv.javacamp2013.Config;
import ch.bbv.javacamp2013.dao.JavacampKeyspace;
import ch.bbv.javacamp2013.dao.TweetDao;
import ch.bbv.javacamp2013.dao.UserDao;
import ch.bbv.javacamp2013.dao.WordSearchDao;
import ch.bbv.javacamp2013.model.Tweet;
import ch.bbv.javacamp2013.model.User;

/**
 * Program to search the tweets for a word.
 */
public final class TweetsForWord {

   private TweetsForWord() {
   }

   /**
    * @param args Command line arguments.
    * @throws IOException If the configuration could not be read.
    */
   public static void main(final String[] args) throws IOException {
      final Config cfg = new Config();
      final JavacampKeyspace javacampKeyspace = new JavacampKeyspace(cfg.getClusterName(), cfg.getClusterAddress());

      final WordSearchDao wordSearch = javacampKeyspace.getWordSearchDao();
      final TweetDao tweetDao = javacampKeyspace.getTweetDao();
      final UserDao userDao = javacampKeyspace.getUserDao();

      final Map<Date, Long> tweetIds = wordSearch.getTweetIdsForWord("markus");

      System.out.println("# of tweets: " + tweetIds.size());
      for (Map.Entry<Date, Long> entry : tweetIds.entrySet()) {
         System.out.println(entry.getKey() + " / " + entry.getValue());
         final Tweet tweet = tweetDao.getTweet(entry.getValue());
         final User user = userDao.getUser(tweet.getUserid());
         System.out.println(user);
         System.out.println(tweet);
      }

   }
}
