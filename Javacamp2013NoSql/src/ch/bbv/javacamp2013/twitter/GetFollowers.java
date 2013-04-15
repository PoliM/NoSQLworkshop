package ch.bbv.javacamp2013.twitter;

import twitter4j.IDs;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class GetFollowers
{

   /**
    * @param args
    */
   public static void main(String[] args)
   {

      ConfigurationBuilder config = new ConfigurationBuilder();
      // config.setUser("bbvjavacamp004");
      // config.setPassword("$bbvjavacamp004");
      config.setOAuthConsumerKey("N06OFZ4IR6oWuvcs60y1Q");
      config.setOAuthConsumerSecret("kNdyvK0wVYNUxTImyPrCj8CUb3nX4pylNLtpBVbM6o");
      config.setOAuthAccessToken("1337468246-eIWZJrwZ62wkSHQLWbXmfv915J9yUGBDbYyAmn8");
      config.setOAuthAccessTokenSecret("viag5HGQGNg0MY7qCr1Fj8AVlKZtMuFsCejbc3pr7s");

      TwitterFactory twitterFactory = new TwitterFactory(config.build());

      Twitter twitter = twitterFactory.getInstance();

      try
      {
         long myId = 96951800; // FC Barcalona twitter.getId();
         System.out.println("myId=" + myId);

         long cursor = -1;

         while (true)
         {
            IDs ids = twitter.getFollowersIDs(myId, cursor);

            long[] idArr = ids.getIDs();
            for (int i = 0; i < idArr.length; i++)
            {
               System.out.println(": " + idArr[i]);
            }
            if (!ids.hasNext())
            {
               break;
            }
            cursor = ids.getNextCursor();
         }
      }
      catch (IllegalStateException e)
      {
         // TODO Auto-generated catch block
         e.printStackTrace();

      }
      catch (TwitterException e)
      {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }

   }

}
