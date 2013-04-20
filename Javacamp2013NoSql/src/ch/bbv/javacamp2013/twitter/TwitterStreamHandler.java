package ch.bbv.javacamp2013.twitter;

import java.util.Date;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;
import ch.bbv.javacamp2013.dao.JavacampKeyspace;
import ch.bbv.javacamp2013.model.Tweet;

/**
 * Program to fetch tweets.
 */
public class TwitterStreamHandler implements StatusListener {
   private static final int MS_PER_S = 1000;

   private static final int REPORT_THRESHOLD = 100;

   private final JavacampKeyspace javacampKeyspace;

   private final TwitterStream twitterStream;

   private final long starttime = System.currentTimeMillis();

   private long count;

   /**
    * Creates a new stream handler.
    * 
    * @param javacampKeyspace The keyspace.
    * @param user The twitter user.
    * @param password The users pw.
    */
   public TwitterStreamHandler(final JavacampKeyspace javacampKeyspace, final String user, final String password) {
      final ConfigurationBuilder config = new ConfigurationBuilder();
      config.setUser(user);
      config.setPassword(password);

      this.javacampKeyspace = javacampKeyspace;

      twitterStream = new TwitterStreamFactory(config.build()).getInstance();

      twitterStream.addListener(this);
   }

   /**
    * Starts to fetch the stream.
    * 
    * @param filter Words to filter for.
    */
   public void stream(final String[] filter) {
      twitterStream.filter(new FilterQuery().track(filter));
   }

   private void addTweetToDB(final Status status) {
      final User user = status.getUser();
      final long userId = user.getId();
      final long tweetId = status.getId();
      final Date createdAt = status.getCreatedAt();
      javacampKeyspace.getUserDao().addUser(
            new ch.bbv.javacamp2013.model.User(userId, user.getName(), user.getScreenName()));
      javacampKeyspace.getTweetDao().addTweet(new Tweet(tweetId, userId, status.getText(), createdAt));
      javacampKeyspace.getUserlineDao().addUserlineEntry(userId, createdAt, tweetId);
   }

   @Override
   public void onStatus(final Status status) {
      // System.out.println("@" + user.getScreenName() + " - " +
      // status.getText());

      addTweetToDB(status);

      count++;
      if (count % REPORT_THRESHOLD == 0) {
         System.out.println(((float) (System.currentTimeMillis() - starttime) / MS_PER_S) + ": " + count);
      }
   }

   @Override
   public void onDeletionNotice(final StatusDeletionNotice statusDeletionNotice) {
      System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
   }

   @Override
   public void onTrackLimitationNotice(final int numberOfLimitedStatuses) {
      System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
   }

   @Override
   public void onScrubGeo(final long userId, final long upToStatusId) {
      System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
   }

   @Override
   public void onStallWarning(final StallWarning warning) {
      System.out.println("Got stall warning:" + warning);
   }

   @Override
   public void onException(final Exception exception) {
      exception.printStackTrace();
   }
}
