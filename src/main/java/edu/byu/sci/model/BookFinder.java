package edu.byu.sci.model;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BookFinder {
    private static final String A_ACCENT = "(?:a|á|&#x[eE]1;|&#aacute;)";
    private static final String CAP_E_ACCENT = "(?:E|É|&#x[cC]9;|&#Eacute;)";
    private static final String E_ACCENT = "(?:e|é|&#x[eE]9;|&#eacute;)";
    private static final String I_ACCENT = "(?:i|í|&#x[eE][dD];|&#iacute;)";
    private static final String O_ACCENT = "(?:o|ó|&#x[fF]3;|&#oacute;)";
    private static final String U_ACCENT = "(?:u|ú|&#x[fF][aA];|&#uacute;)";
    private static final String SPACE = "(?:\\s| |&#x[aA]0;|&#160;)+";

    private static final String JOHN_JUAN = "(John|Juan)";
    private static final int NEW_TESTAMENT_INDEX = 39;
    private static final int BOOK_OF_MORMON_INDEX = 66;
    private static final int DOCTRINE_AND_COVENANTS_INDEX = 85;
    private static final int PEARL_OF_GREAT_PRICE_INDEX = 88;

    private final List<String> abbreviations = List.of("gen", "ex", "lev", "num", "deut", "josh", "judg", "ruth",
            "1-sam", "2-sam", "1-kgs", "2-kgs", "1-chr", "2-chr", "ezra", "neh", "esth", "job", "ps", "prov", "eccl",
            "song", "isa", "jer", "lam", "ezek", "dan", "hosea", "joel", "amos", "obad", "jonah", "micah", "nahum",
            "hab", "zeph", "hag", "zech", "mal", "matt", "mark", "luke", "john", "acts", "rom", "1-cor", "2-cor", "gal",
            "eph", "philip", "col", "1-thes", "2-thes", "1-tim", "2-tim", "titus", "philem", "heb", "james", "1-pet",
            "2-pet", "1-jn", "2-jn", "3-jn", "jude", "rev", "bofm-title", "bofm-intro", "three", "eight", "1-ne",
            "2-ne", "jacob", "enos", "jarom", "omni", "w-of-m", "mosiah", "alma", "hel", "3-ne", "4-ne", "morm",
            "ether", "moro", "dc-intro", "dc", "od", "moses", "abr", "js-m", "js-h", "a-of-f"

    );

    private final List<String> bookPatterns = List.of("(Genesis|G" + E_ACCENT + "nesis)",
            "(Exodus|" + CAP_E_ACCENT + "xodo)", "(Leviticus|Lev" + I_ACCENT + "tico)",
            "(Numbers|N" + U_ACCENT + "meros)", "(Deuteronomy|Deuteronomio)", "(Joshua|Josu" + "eAccent)",
            "(Judges|Jueces)", "(Ruth|Rut)", "1" + SPACE + "Samuel", "2" + SPACE + "Samuel",
            "1" + SPACE + "(Kings|Reyes)", "2" + SPACE + "(Kings|Reyes)",
            "1" + SPACE + "(Chronicles|Cr" + O_ACCENT + "nicas)", "2" + SPACE + "(Chronicles|Cr" + O_ACCENT + "nicas)",
            "(Ezra|Esdras)", "(Nehemiah|Nehem" + I_ACCENT + "as)", "(Esther|Ester)", "Job", "(Psalms?|Salmos?)",
            "(Proverbs|Proverbios)", "(Ecclesiastes|Eclesiast" + E_ACCENT + "s)",
            "(Song" + SPACE + "of" + SPACE + "Solomon|Cantares)", "(Isaiah|Isa" + I_ACCENT + "as)",
            "(Jeremiah|Jerem" + I_ACCENT + "as)", "(Lamentations|Lamentaciones)", "(Ezekiel|Ezequiel)", "Daniel",
            "(Hosea|Oseas)", "Joel", "(Amos|Am" + O_ACCENT + "s)", "(Obadiah|Abd" + I_ACCENT + "as)",
            "(Jonah|Jon" + A_ACCENT + "s)", "(Micah|Miqueas)", "(Nahum|Nah" + U_ACCENT + "m)", "(Habakkuk|Habacuc)",
            "(Zephaniah|Sofon" + I_ACCENT + "as)", "(Haggai|Hageo)", "(Zechariah|Zacar" + I_ACCENT + "as)",
            "(Malachi|Malaqu" + I_ACCENT + "as)", "(Matthew|Mateo)", "(Mark|Marcos)", "(Luke|Lucas)", JOHN_JUAN,
            "(Acts|Hechos)", "(Romans|Romanos)", "1" + SPACE + "(Corinthians|Corintios)",
            "2" + SPACE + "(Corinthians|Corintios)", "(Galatians|G" + A_ACCENT + "latas)", "(Ephesians|Efesios)",
            "(Philippians|Filipenses)", "(Colossians|Colosenses)", "1" + SPACE + "(Thessalonians|Tesalonicenses)",
            "2" + SPACE + "(Thessalonians|Tesalonicenses)", "1" + SPACE + "(Timothy|Timoteo)",
            "2" + SPACE + "(Timothy|Timoteo)", "(Titus|Tito)", "(Philememon|Filem" + O_ACCENT + "n)",
            "(Hebrews|Hebreos)", "(James|Santiago)", "1" + SPACE + "(Peter|Pedro)", "2" + SPACE + "(Peter|Pedro)",
            "1" + SPACE + JOHN_JUAN, "2" + SPACE + JOHN_JUAN, "3" + SPACE + JOHN_JUAN, "(Jude|Judas)",
            "(Revelation|Apocalipsis)",
            // NEEDSWORK: decide if we'll need to parse these...
            "bofm-title", "(bofm-intro|introduction)", "three", "eight", "1-ne", "2-ne", "jacob", "enos", "jarom", "omni", "w-of-m",
            "mosiah", "alma", "hel", "3-ne", "4-ne", "morm", "ether", "moro", "(dc-intro|introduction)", "dc", "od", "moses",
            "abr", "js-m", "js-h", "a-of-f");

    public static final BookFinder sInstance = new BookFinder();

    private BookFinder() {
        // Make private for singleton pattern
    }

    public String abbreviationForBook(String book) {
        int index = indexOfBook(book);

        return abbreviations.get(index);
    }

    public int bookIdForBook(String book) {
        int index = indexOfBook(book.startsWith("jst-") ? book.substring(4) : book);

        if (index < 0) {
            Logger.getLogger(BookFinder.class.getName()).log(Level.WARNING,
                    () -> "Unable to find index of book '" + book + "'");
        }

        if (index >= PEARL_OF_GREAT_PRICE_INDEX) {
            return index + 400 - PEARL_OF_GREAT_PRICE_INDEX + 1;
        } else if (index >= DOCTRINE_AND_COVENANTS_INDEX) {
            return index + 300 - DOCTRINE_AND_COVENANTS_INDEX + 1;
        } else if (index >= BOOK_OF_MORMON_INDEX) {
            return index + 200 - BOOK_OF_MORMON_INDEX + 1;
        } else {
            return index + 100 + 1;
        }
    }

    public int indexOfBook(String book) {
        for (int i = 0; i < bookPatterns.size(); i++) {
            Matcher matcher = Pattern.compile(bookPatterns.get(i), Pattern.CASE_INSENSITIVE).matcher(book);

            if (matcher.matches()) {
                return i;
            }

            if (book.equalsIgnoreCase(abbreviations.get(i))) {
                return i;
            }
        }

        return -1;
    }

    public String volumeForBook(String book) {
        int index = indexOfBook(book);

        if (index < NEW_TESTAMENT_INDEX) {
            return "ot";
        } else if (index < BOOK_OF_MORMON_INDEX) {
            return "nt";
        } else if (index < DOCTRINE_AND_COVENANTS_INDEX) {
            return "bofm";
        } else if (index < PEARL_OF_GREAT_PRICE_INDEX) {
            return "dc-testament";
        } else {
            return "pgp";
        }
    }
}
