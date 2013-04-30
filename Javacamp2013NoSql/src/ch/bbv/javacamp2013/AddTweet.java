package ch.bbv.javacamp2013;

import java.io.IOException;
import java.util.Date;

import ch.bbv.javacamp2013.dao.JavacampKeyspace;
import ch.bbv.javacamp2013.model.Tweet;
import ch.bbv.javacamp2013.model.User;

public class AddTweet {

   public static void main(String[] args) throws IOException {

      final Config cfg = new Config();
      System.out.println("Connecting to cluster " + cfg.getClusterName() + " @ " + cfg.getClusterAddress());
      final JavacampKeyspace javacampKeyspace = new JavacampKeyspace(cfg.getClusterName(), cfg.getClusterAddress());

      long userid = System.currentTimeMillis();
      long tweetid = userid + 1;

      User user = new User(userid, "jcuser", "javacamp user");

      Tweet tweet = new Tweet(tweetid, userid, "The javacamp is great!", new Date());

      javacampKeyspace.addTweet(user, tweet);

      System.out.println("added " + tweet + " from " + user);
   }
}
