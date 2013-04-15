package ch.bbv.javacamp2013.queries;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.TreeSet;

public class CommonWords
{
   private final Set<String> words = new TreeSet<>();

   public CommonWords() throws IOException
   {
      InputStream is = this.getClass().getResourceAsStream("english-words.95");
      BufferedReader br = new BufferedReader(new InputStreamReader(is));

      String line = br.readLine();
      while (line != null)
      {
         words.add(line.trim());
         line = br.readLine();
      }
   }

   public boolean hasWord(String word)
   {
      return words.contains(word);
   }
}
