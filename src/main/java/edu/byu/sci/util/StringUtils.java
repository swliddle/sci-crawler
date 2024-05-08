package edu.byu.sci.util;

import java.text.Normalizer;

public class StringUtils {
    private StringUtils() {
        // Ignore
    }

    public static String allDecodedEntities(String string) {
        return string
                .replace("&ndash;", "–")
                .replace("&mdash;", "—")
                .replace("&lsquo;", "‘")
                .replace("&rsquo;", "’")
                .replace("&ldquo;", "“")
                .replace("&rdquo;", "”")
                .replace("&bull;", "•")
                .replace("&hellip;", "…")
                .replace("&#257;", "ā")
                .replace("&Aring;", "Å")
                .replace("&agrave;", "à")
                .replace("&aacute;", "á")
                .replace("&acirc;", "â")
                .replace("&atilde;", "ã")
                .replace("&auml;", "ä")
                .replace("&aring;", "å")
                .replace("&aelig;", "æ")
                .replace("&ccedil;", "ç")
                .replace("&egrave;", "è")
                .replace("&eacute;", "é")
                .replace("&ecirc;", "ê")
                .replace("&euml;", "ë")
                .replace("&igrave;", "ì")
                .replace("&iacute;", "í")
                .replace("&icirc;", "î")
                .replace("&iuml;", "ï")
                .replace("&ntilde;", "ñ")
                .replace("&ograve;", "ò")
                .replace("&oacute;", "ó")
                .replace("&ocirc;", "ô")
                .replace("&otilde;", "õ")
                .replace("&ouml;", "ö")
                .replace("&divide;", "÷")
                .replace("&oslash;", "ø")
                .replace("&ucirc;", "û")
                .replace("&uacute;", "ú")
                .replace("&ugrave;", "ù")
                .replace("&uuml;", "ü")
                .replace("&yacute;", "ý")
                .replace("&yuml;", "ÿ")
                .replace("&thorn;", "þ")
                .replace("&nbsp;", " ")
                .replace("&iexcl;", "¡")
                .replace("&cent;", "¢")
                .replace("&copy;", "©")
                .replace("&reg;", "®")
                .replace("&deg;", "°")
                .replace("&frac12;", "½")
                .replace("&iquest;", "¿")
                .replace("&#xA0;", " ")
                .replace("&#x2013;", "–")
                .replace("&#x2014;", "—")
                .replace("&#x2018;", "‘")
                .replace("&#x2019;", "’")
                .replace("&#x201C;", "“")
                .replace("&#x201D;", "”")
                .replace("&#x2026;", "…");
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

    public static String emptyIfNull(String string) {
        return string == null ? "" : string;
    }

    public static String encodeSpecialCharacters(String text) {
        return text
                .replace("–", "&#x2013;")
                .replace("—", "&#x2014;")
                .replace("‘", "&#x2018;")
                .replace("’", "&#x2019;")
                .replace("“", "&#x201C;")
                .replace("”", "&#x201D;")
                .replace("…", "&#x2026;");
    }

    public static boolean isEmpty(String string) {
        return string == null || string.length() <= 0;
    }

    public static String removeAccents(String input) {
        return Normalizer.normalize(input, Normalizer.Form.NFD)
                .replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
    }
}
