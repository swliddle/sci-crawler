package edu.byu.sci.crawler;

import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SortByNote implements Comparator<String> {
    private static final Logger logger = Logger.getLogger(SortByNote.class.getName());

    @Override
    public int compare(String arg0, String arg1) {
        if (arg0 != null && arg1 != null && arg0.length() > 4 && arg1.length() > 4) {
            try {
                int n0 = Integer.parseInt(arg0.substring(4));
                int n1 = Integer.parseInt(arg1.substring(4));

                return n0 - n1;
            } catch (NumberFormatException e) {
                logger.log(Level.WARNING, "Unable to sort items (NumberFormatException)");
            }
        }

        return (arg0 != null && arg1 != null) ? arg0.compareTo(arg1) : 0;
    }
}
