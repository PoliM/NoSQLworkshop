package ch.bbv.javacamp2013.model;

public class User
{
   private final long userid;
   private final String name;
   private final String screenName;

   public User(long userid, String name, String screenName)
   {
      this.userid = userid;
      this.name = name;
      this.screenName = screenName;
   }

   public long getUserid()
   {
      return userid;
   }

   public String getName()
   {
      return name;
   }

   public String getScreenName()
   {
      return screenName;
   }

   @Override
   public String toString()
   {
      return "User [userid=" + userid + ", name=" + name + ", screenName=" + screenName + "]";
   }
}
