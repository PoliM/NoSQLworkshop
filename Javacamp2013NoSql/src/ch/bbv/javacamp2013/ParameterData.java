package ch.bbv.javacamp2013;

/**
 * The access parameter for one of our twitter accounts.
 */
public class ParameterData {

   private final String username;

   private final String password;

   private final String[] filter;

   /**
    * Creates a new parameter.
    * 
    * @param username Name of the twiter user.
    * @param password Its password.
    * @param filter All the words to filter.
    */
   public ParameterData(final String username, final String password, final String[] filter) {
      this.username = username;
      this.password = password;
      this.filter = filter.clone();
   }

   public String getUsername() {
      return username;
   }

   public String getPassword() {
      return password;
   }

   public String[] getFilter() {
      return filter.clone();
   }

   @Override
   public String toString() {
      final StringBuffer str = new StringBuffer(128);
      str.append("Parameter(").append(username).append(", [");
      boolean isFirst = true;
      for (String word : filter) {
         if (isFirst) {
            isFirst = false;
         }
         else {
            str.append(", ");
         }
         str.append(word);
      }
      str.append("])");
      return str.toString();
   }
}
