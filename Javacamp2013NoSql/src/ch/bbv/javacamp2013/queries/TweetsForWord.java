package ch.bbv.javacamp2013.queries;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import ch.bbv.javacamp2013.Config;
import ch.bbv.javacamp2013.dao.JavacampKeyspace;
import ch.bbv.javacamp2013.dao.TweetDao;
import ch.bbv.javacamp2013.dao.WordSearchDao;
import ch.bbv.javacamp2013.model.Tweet;

public class TweetsForWord
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

      TreeMap<Date, Long> tweetIds = wordSearch.getTweetIdsForWord("password");

      for (Map.Entry<Date, Long> entry : tweetIds.entrySet())
      {
         System.out.println(entry.getKey() + " / " + entry.getValue());
         Tweet tweet = tweetDao.getTweet(entry.getValue());
         System.out.println(tweet);
      }
   }
}
