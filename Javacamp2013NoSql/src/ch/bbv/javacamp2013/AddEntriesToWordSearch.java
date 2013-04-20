package ch.bbv.javacamp2013;

import java.io.IOException;
import java.util.Date;
import java.util.Locale;

import ch.bbv.javacamp2013.dao.JavacampKeyspace;
import ch.bbv.javacamp2013.dao.TweetDao;
import ch.bbv.javacamp2013.dao.TweetDao.TweetIterator;

/**
 * This programm goes through all Tweets and creates the WordSearch entries for
 * them.
 */
public final class AddEntriesToWordSearch {

   private static final int OUTPUT_THRESHOLD = 1000;

   /**
    * Because this must never be instantiated.
    */
   private AddEntriesToWordSearch() {
   }

   /**
    * Starts the program. Blah until end.
    * 
    * @param args Command line arguments.
    * @throws IOException When the configuration wasn't readable.
    */
   public static void main(final String[] args) throws IOException {
      final Config cfg = new Config();
      System.out.println("Connecting to cluster " + cfg.getClusterName() + " @ " + cfg.getClusterAddress());
      final JavacampKeyspace javacampKeyspace = new JavacampKeyspace(cfg.getClusterName(), cfg.getClusterAddress());

      long count = 0;

      final TweetDao tweetAccess = javacampKeyspace.getTweetDao();
      final TweetIterator iter = tweetAccess.getIterator();
      while (iter.moveNextSkipEmptyRow()) {
         final Date createdAt = iter.getCreatedAt();
         final Long key = iter.getKey();

         final String[] data = iter.getBody().split("\\W");
         for (int j = 0; j < data.length; j++) {
            String word = data[j];
            if (word.length() > 1) {
               word = word.toLowerCase(Locale.getDefault());
               // System.out.print(word + " ");
               javacampKeyspace.getWordSearchDao().addWordEntry(word, createdAt, key);
            }
         }
         count++;
         if (count % OUTPUT_THRESHOLD == 0) {
            System.out.println(System.currentTimeMillis() + ": added " + count);
         }
         // System.out.println(":" + createdAt + " ->" + key);
      }
      System.out.println("Total=" + count);
   }
}
