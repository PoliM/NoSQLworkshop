package ch.bbv.javacamp2013.model;

/**
 * The data of a user.
 */
public class User {
   private final long userid;
   private final String name;
   private final String screenName;

   /**
    * Creates a fully initialized user.
    * 
    * @param userid The user id.
    * @param name The name.
    * @param screenName The display name.
    */
   public User(final long userid, final String name, final String screenName) {
      this.userid = userid;
      this.name = name;
      this.screenName = screenName;
   }

   public long getUserid() {
      return userid;
   }

   public String getName() {
      return name;
   }

   public String getScreenName() {
      return screenName;
   }

   @Override
   public String toString() {
      return "User [userid=" + userid + ", name=" + name + ", screenName=" + screenName + "]";
   }
}
