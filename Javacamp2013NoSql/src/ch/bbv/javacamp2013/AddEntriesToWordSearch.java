package ch.bbv.javacamp2013;

import java.util.Date;

import ch.bbv.javacamp2013.dao.JavacampKeyspace;
import ch.bbv.javacamp2013.dao.TweetDao;
import ch.bbv.javacamp2013.dao.TweetDao.TweetIterator;

public class AddEntriesToWordSearch
{

   private static String CLUSTER_ADRESS = "192.168.56.101:9160";

   private static final String CLUSTER_NAME = "Test Cluster";

   /**
    * @param args
    */
   public static void main(String[] args)
   {
      System.out.println("Connecting to cluster " + CLUSTER_NAME + " @ " + CLUSTER_ADRESS);
      JavacampKeyspace javacampKeyspace = new JavacampKeyspace(CLUSTER_NAME, CLUSTER_ADRESS);

      long count = 0;

      TweetDao tweetAccess = javacampKeyspace.getTweetDao();
      TweetIterator i = tweetAccess.getIterator();
      while (i.moveNextSkipEmptyRow())
      {
         Date createdAt = i.getCreatedAt();
         Long key = i.getKey();

         String[] data = i.getBody().split("\\W");
         for (int j = 0; j < data.length; j++)
         {
            String word = data[j];
            if (word.length() > 1)
            {
               word = word.toLowerCase();
               // System.out.print(word + " ");
               javacampKeyspace.getWordSearchDao().addWordEntry(word, createdAt, key);
            }
         }
         count++;
         if (count % 100 == 0)
         {
            System.out.println(System.currentTimeMillis() + ": added " + count);
         }
         // System.out.println(":" + createdAt + " ->" + key);
      }
   }
}
