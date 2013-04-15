package ch.bbv.javacamp2013;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;

import ch.bbv.javacamp2013.dao.JavacampKeyspace;
import ch.bbv.javacamp2013.dao.TweetDao;
import ch.bbv.javacamp2013.dao.TweetDao.TweetIterator;

public class AddEntriesToWordSearch
{
   /**
    * @param args
    * @throws IOException
    * @throws FileNotFoundException
    */
   public static void main(String[] args) throws FileNotFoundException, IOException
   {
      Config cfg = new Config();
      System.out.println("Connecting to cluster " + cfg.getClusterName() + " @ " + cfg.getClusterAddress());
      JavacampKeyspace javacampKeyspace = new JavacampKeyspace(cfg.getClusterName(), cfg.getClusterAddress());

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
         if (count % 1000 == 0)
         {
            System.out.println(System.currentTimeMillis() + ": added " + count);
         }
         // System.out.println(":" + createdAt + " ->" + key);
      }
      System.out.println("Total=" + count);
   }
}
