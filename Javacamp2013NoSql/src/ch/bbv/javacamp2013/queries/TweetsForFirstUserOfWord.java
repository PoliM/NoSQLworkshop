package ch.bbv.javacamp2013.queries;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import ch.bbv.javacamp2013.Config;
import ch.bbv.javacamp2013.dao.JavacampKeyspace;
import ch.bbv.javacamp2013.dao.TweetDao;
import ch.bbv.javacamp2013.dao.UserDao;
import ch.bbv.javacamp2013.dao.UserlineDao;
import ch.bbv.javacamp2013.dao.WordSearchDao;
import ch.bbv.javacamp2013.model.Tweet;
import ch.bbv.javacamp2013.model.User;

public class TweetsForFirstUserOfWord
{

   /**
    * @param args
    * @throws IOException
    * @throws FileNotFoundException
    */
   public static void main(String[] args) throws FileNotFoundException, IOException
   {
      Config cfg = new Config();
      JavacampKeyspace javacampKeyspace = new JavacampKeyspace(cfg.getClusterName(), cfg.getClusterAddress());

      WordSearchDao wordSearch = javacampKeyspace.getWordSearchDao();
      TweetDao tweetDao = javacampKeyspace.getTweetDao();
      UserDao userDao = javacampKeyspace.getUserDao();
      UserlineDao userlineDao = javacampKeyspace.getUserlineDao();

      TreeMap<Date, Long> tweetIds = wordSearch.getTweetIdsForWord("schweiz");

      if (tweetIds.size() > 0)
      {
         long tweetId = tweetIds.entrySet().iterator().next().getValue();

         Tweet tweet = tweetDao.getTweet(tweetId);
         System.out.println(tweet);

         User user = userDao.getUser(tweet.getUserid());
         System.out.println(user);

         List<Tweet> userline = userlineDao.getUserline(user.getUserid(), tweetDao);
         System.out.println("# of tweets: " + userline.size());
         for (Tweet tweet2 : userline)
         {
            System.out.println(tweet2);
         }
      }
   }
}
