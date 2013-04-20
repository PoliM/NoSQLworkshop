package ch.bbv.javacamp2013.twitter;

import twitter4j.IDs;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Program to fetch followers.
 */
public final class GetFollowers {

   // twitter.getId();
   private static final int FC_BARCELONA_TWITTER_ID = 96951800;

   private GetFollowers() {
   }

   /**
    * @param args Command line arguments.
    */
   public static void main(final String[] args) {

      final ConfigurationBuilder config = new ConfigurationBuilder();
      // config.setUser("bbvjavacamp004");
      // config.setPassword("$bbvjavacamp004");
      config.setOAuthConsumerKey("N06OFZ4IR6oWuvcs60y1Q");
      config.setOAuthConsumerSecret("kNdyvK0wVYNUxTImyPrCj8CUb3nX4pylNLtpBVbM6o");
      config.setOAuthAccessToken("1337468246-eIWZJrwZ62wkSHQLWbXmfv915J9yUGBDbYyAmn8");
      config.setOAuthAccessTokenSecret("viag5HGQGNg0MY7qCr1Fj8AVlKZtMuFsCejbc3pr7s");

      final TwitterFactory twitterFactory = new TwitterFactory(config.build());

      final Twitter twitter = twitterFactory.getInstance();

      try {
         final long myId = FC_BARCELONA_TWITTER_ID;
         System.out.println("myId=" + myId);

         long cursor = -1;

         while (true) {
            final IDs ids = twitter.getFollowersIDs(myId, cursor);

            final long[] idArr = ids.getIDs();
            for (int i = 0; i < idArr.length; i++) {
               System.out.println(": " + idArr[i]);
            }
            if (!ids.hasNext()) {
               break;
            }
            cursor = ids.getNextCursor();
         }
      }
      catch (IllegalStateException e) {
         e.printStackTrace();

      }
      catch (TwitterException e) {
         e.printStackTrace();
      }

   }

}
