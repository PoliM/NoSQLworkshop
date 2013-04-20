package ch.bbv.javacamp2013.queries;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.TreeSet;

/**
 * A class that holds a dictionary of common words. In this case it reads the
 * words from english-words.95.
 */
public class CommonWords {
   private final Set<String> words = new TreeSet<>();

   /**
    * Creates a new instance.
    * 
    * @throws IOException If the word list could not be read.
    */
   public CommonWords() throws IOException {
      final InputStream is = this.getClass().getResourceAsStream("english-words.95");
      final BufferedReader br = new BufferedReader(new InputStreamReader(is));

      String line = br.readLine();
      while (line != null) {
         words.add(line.trim());
         line = br.readLine();
      }
   }

   /**
    * Checks a word.
    * 
    * @param word The word.
    * @return True if the word is in the dictionary.
    */
   public boolean hasWord(final String word) {
      return words.contains(word);
   }
}
