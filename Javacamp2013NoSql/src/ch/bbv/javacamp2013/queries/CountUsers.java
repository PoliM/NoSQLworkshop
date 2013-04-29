package ch.bbv.javacamp2013.queries;

import java.io.IOException;

import ch.bbv.javacamp2013.Config;
import ch.bbv.javacamp2013.dao.JavacampKeyspace;
import ch.bbv.javacamp2013.dao.UserDao;
import ch.bbv.javacamp2013.dao.UserDao.UserIterator;

/**
 * Counts the users.
 */
public class CountUsers {

   private static final int REPORT_THRESHOLD = 10000;

   /**
    * Starts to count users.
    * 
    * @param args The command line arguments.
    * @throws IOException If the configuration could not ber read.
    */
   public static void main(String[] args) throws IOException {
      final Config cfg = new Config();
      System.out.println("Connecting to cluster " + cfg.getClusterName() + " @ " + cfg.getClusterAddress());
      final UserDao userAccess = new JavacampKeyspace(cfg.getClusterName(), cfg.getClusterAddress()).getUserDao();

      int count = 0;
      final UserIterator iter = userAccess.getIterator();
      while (iter.moveNextSkipEmptyRow()) {
         if (count % REPORT_THRESHOLD == 0) {
            System.out.println(count + ": " + iter.getKey() + ": userid=" + iter.getUserId() + ", name=\""
                  + iter.getName() + "\", screenName=" + iter.getScreenName());
         }
         count++;
      }
      System.out.println("Total=" + count);

   }

}
