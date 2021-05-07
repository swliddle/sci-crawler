package edu.byu.sci.crawler;

public class StringUtils {
    private StringUtils() {
        // Ignore
    }

    public static String decodedEntities(String string) {
        return string
                .replaceAll("&aacute;", "á")
                .replaceAll("&eacute;", "é")
                .replaceAll("&iacute;", "í")
                .replaceAll("&oacute;", "ó")
                .replaceAll("&uacute;", "ú")
                .replaceAll("&ntilde;", "ñ");
    }
}
