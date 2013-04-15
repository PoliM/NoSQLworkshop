package ch.bbv.javacamp2013.queries;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import ch.bbv.javacamp2013.Config;
import ch.bbv.javacamp2013.dao.JavacampKeyspace;
import ch.bbv.javacamp2013.dao.WordSearchDao;
import ch.bbv.javacamp2013.dao.WordSearchDao.WordIterator;

public class WordList
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

      Set<String> words = new TreeSet<>();
      Set<String> wordsInCommon = new TreeSet<>();

      CommonWords commonWords = new CommonWords();

      WordSearchDao wordSearch = javacampKeyspace.getWordSearchDao();
      int count = 0;
      WordIterator i = wordSearch.getIterator();
      while (i.moveNextSkipEmptyRow())
      {
         words.add(i.getKey());

         if (commonWords.hasWord(i.getKey()))
         {
            wordsInCommon.add(i.getKey());
         }

         if (count % 1000 == 0)
         {
            System.out.println(count + ": " + i.getKey());
         }
         count++;
      }

      String currentPrefix = "";
      for (String word : wordsInCommon)
      {
         String prefix = word.substring(0, 2);
         if (!prefix.equals(currentPrefix))
         {
            currentPrefix = prefix;
            System.out.println();
            System.out.print(word);
         }
         else
         {
            System.out.print(", ");
            System.out.print(word);
         }
      }
      System.out.println();
      System.out.println("Total=" + count);
      System.out.println("In common=" + wordsInCommon.size());
   }
}
