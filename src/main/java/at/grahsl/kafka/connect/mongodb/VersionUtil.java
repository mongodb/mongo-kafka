package at.grahsl.kafka.connect.mongodb;

class VersionUtil {
  public static String getVersion() {
    try {
      return VersionUtil.class.getPackage().getImplementationVersion();
    } catch(Exception ex){
      return "?VersionUnknown?";
    }
  }
}
