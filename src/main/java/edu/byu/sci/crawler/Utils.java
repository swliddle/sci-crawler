package edu.byu.sci.crawler;

public class Utils {
    private Utils() {
    }

    public static int integerValue(String text) {
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
