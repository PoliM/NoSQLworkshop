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

public class TwitterStreamHandler implements StatusListener
{
   private final JavacampKeyspace javacampKeyspace;

   private final TwitterStream twitterStream;

   private final long starttime = System.currentTimeMillis();

   private long count = 0;

   public TwitterStreamHandler(JavacampKeyspace javacampKeyspace, String user, String password)
   {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.setUser(user);
      config.setPassword(password);

      this.javacampKeyspace = javacampKeyspace;

      twitterStream = new TwitterStreamFactory(config.build()).getInstance();

      twitterStream.addListener(this);
   }

   public void stream(String[] filter)
   {
      twitterStream.filter(new FilterQuery().track(filter));
   }

   private void addTweetToDB(Status status)
   {
      User user = status.getUser();
      long userId = user.getId();
      long tweetId = status.getId();
      Date createdAt = status.getCreatedAt();
      javacampKeyspace.getUserDao().addUser(userId, user.getName(), user.getScreenName());
      javacampKeyspace.getTweetDao().addTweet(tweetId, userId, status.getText(), createdAt);
      javacampKeyspace.getUserlineDao().addUserlineEntry(userId, createdAt, tweetId);
   }

   @Override
   public void onStatus(Status status)
   {
      // System.out.println("@" + user.getScreenName() + " - " +
      // status.getText());

      addTweetToDB(status);

      count++;
      if (count % 100 == 0)
      {
         System.out.println(((float) (System.currentTimeMillis() - starttime) / 1000) + ": " + count);
      }
   }

   @Override
   public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice)
   {
      System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
   }

   @Override
   public void onTrackLimitationNotice(int numberOfLimitedStatuses)
   {
      System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
   }

   @Override
   public void onScrubGeo(long userId, long upToStatusId)
   {
      System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
   }

   @Override
   public void onStallWarning(StallWarning warning)
   {
      System.out.println("Got stall warning:" + warning);
   }

   @Override
   public void onException(Exception ex)
   {
      ex.printStackTrace();
   }
}
