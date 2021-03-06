package ch.bbv.javacamp2013.model;

import java.util.Date;

/**
 * Data of a tweeet.
 */
public class Tweet {
   private final long tweetid;
   private final long userid;
   private final String body;
   private final Date createdAt;

   /**
    * Creates a fully initialized tweet.
    * 
    * @param tweetid The id.
    * @param userid The user id.
    * @param body The body text.
    * @param createdAt The creation date.
    */
   public Tweet(final long tweetid, final long userid, final String body, final Date createdAt) {
      this.tweetid = tweetid;
      this.userid = userid;
      this.body = body;
      this.createdAt = createdAt;
   }

   public long getTweetid() {
      return tweetid;
   }

   public long getUserid() {
      return userid;
   }

   public String getBody() {
      return body;
   }

   public Date getCreatedAt() {
      return createdAt;
   }

   @Override
   public String toString() {
      return "Tweet [tweetid=" + tweetid + ", userid=" + userid + ", body=" + body + ", createdAt=" + createdAt + "]";
   }
}
