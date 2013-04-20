package ch.bbv.javacamp2013.queries;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import ch.bbv.javacamp2013.Config;
import ch.bbv.javacamp2013.dao.JavacampKeyspace;
import ch.bbv.javacamp2013.dao.WordSearchDao;
import ch.bbv.javacamp2013.dao.WordSearchDao.WordIterator;

/**
 * Program to get all the words in the search index.
 */
public final class WordList {

   private static final int REPORT_THRESHOLD = 1000;

   private WordList() {
   }

   /**
    * @param args Command line arguments.
    * @throws IOException If the configuration could not be read.
    */
   public static void main(final String[] args) throws IOException {
      final Config cfg = new Config();
      final JavacampKeyspace javacampKeyspace = new JavacampKeyspace(cfg.getClusterName(), cfg.getClusterAddress());

      final Set<String> words = new TreeSet<>();
      final Set<String> wordsInCommon = new TreeSet<>();

      final CommonWords commonWords = new CommonWords();

      final WordSearchDao wordSearch = javacampKeyspace.getWordSearchDao();
      int count = 0;
      final WordIterator i = wordSearch.getIterator();
      while (i.moveNextSkipEmptyRow()) {
         words.add(i.getKey());

         if (commonWords.hasWord(i.getKey())) {
            wordsInCommon.add(i.getKey());
         }

         if (count % REPORT_THRESHOLD == 0) {
            System.out.println(count + ": " + i.getKey());
         }
         count++;
      }

      String currentPrefix = "";
      for (String word : wordsInCommon) {
         final String prefix = word.substring(0, 2);
         if (!prefix.equals(currentPrefix)) {
            currentPrefix = prefix;
            System.out.println();
            System.out.print(word);
         }
         else {
            System.out.print(", ");
            System.out.print(word);
         }
      }
      System.out.println();
      System.out.println("Total=" + count);
      System.out.println("In common=" + wordsInCommon.size());
   }
}
