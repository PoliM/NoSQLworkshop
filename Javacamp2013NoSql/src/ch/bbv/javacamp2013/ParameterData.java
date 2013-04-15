package ch.bbv.javacamp2013;

public class ParameterData
{

   private final String _username;

   private final String _password;

   private final String[] _filter;

   public ParameterData(String username, String password, String[] filter)
   {
      _username = username;
      _password = password;
      _filter = filter;
   }

   public String getUsername()
   {
      return _username;
   }

   public String getPassword()
   {
      return _password;
   }

   public String[] getFilter()
   {
      return _filter;
   }

   @Override
   public String toString()
   {
      StringBuffer str = new StringBuffer();
      str.append("Parameter(").append(_username).append(", [");
      boolean isFirst = true;
      for (String word : _filter)
      {
         if (isFirst)
         {
            isFirst = false;
         }
         else
         {
            str.append(", ");
         }
         str.append(word);
      }
      str.append("])");
      return str.toString();
   }
}
