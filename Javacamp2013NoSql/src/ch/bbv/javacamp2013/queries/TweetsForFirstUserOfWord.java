package ch.bbv.javacamp2013.queries;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import ch.bbv.javacamp2013.Config;
import ch.bbv.javacamp2013.dao.JavacampKeyspace;
import ch.bbv.javacamp2013.dao.TweetDao;
import ch.bbv.javacamp2013.dao.UserDao;
import ch.bbv.javacamp2013.dao.UserlineDao;
import ch.bbv.javacamp2013.dao.WordSearchDao;
import ch.bbv.javacamp2013.model.Tweet;
import ch.bbv.javacamp2013.model.User;

/**
 * Program that reads the tweets of a user.
 */
public final class TweetsForFirstUserOfWord {

   private TweetsForFirstUserOfWord() {
   }

   /**
    * @param args The command line arguments.
    * @throws IOException If the configuration could not be read.
    */
   public static void main(final String[] args) throws IOException {
      final Config cfg = new Config();
      final JavacampKeyspace javacampKeyspace = new JavacampKeyspace(cfg.getClusterName(), cfg.getClusterAddress());

      final WordSearchDao wordSearch = javacampKeyspace.getWordSearchDao();
      final TweetDao tweetDao = javacampKeyspace.getTweetDao();
      final UserDao userDao = javacampKeyspace.getUserDao();
      final UserlineDao userlineDao = javacampKeyspace.getUserlineDao();

      final Map<Date, Long> tweetIds = wordSearch.getTweetIdsForWord("markus");

      if (!tweetIds.isEmpty()) {
         final long tweetId = tweetIds.entrySet().iterator().next().getValue();

         final Tweet tweet = tweetDao.getTweet(tweetId);
         System.out.println(tweet);

         final User user = userDao.getUser(tweet.getUserid());
         System.out.println(user);

         final List<Tweet> userline = userlineDao.getUserline(user.getUserid(), tweetDao);
         System.out.println("# of tweets: " + userline.size());
         for (Tweet tweet2 : userline) {
            System.out.println(tweet2);
         }
      }
   }
}
