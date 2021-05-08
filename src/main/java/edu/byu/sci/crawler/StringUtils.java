package edu.byu.sci.crawler;

public class StringUtils {
    private StringUtils() {
        // Ignore
    }

    public static String decodedEntities(String string) {
        return string
                .replace("&aacute;", "á")
                .replace("&eacute;", "é")
                .replace("&iacute;", "í")
                .replace("&oacute;", "ó")
                .replace("&uacute;", "ú")
                .replace("&ntilde;", "ñ");
    }
}
