package edu.byu.sci.crawler;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONObject;

import edu.byu.sci.database.Database;
import edu.byu.sci.database.SearchTerm;
import edu.byu.sci.model.BookFinder;
import edu.byu.sci.model.Link;
import edu.byu.sci.model.SortByNote;
import edu.byu.sci.model.Speakers;
import edu.byu.sci.util.FileUtils;
import edu.byu.sci.util.StringUtils;
import edu.byu.sci.util.Utils;

public class SciCrawler {

    /*
     * NEEDSWORK:
     * - Speaker info needs updating for Jr./Sr. etc. -- update database?
     * - Need to rewrite visible text of chapter links we've expanded (with verse
     * range for chapter).
     * - Footnotes not getting inlined after 12 or 15 or so.
     */

    // Constants
    public static final String HYPHEN = "-";
    public static final String NDASH = "–";
    public static final String PATH_SEPARATOR = "/";
    public static final String URL_SCRIPTURE_PATH = "/study/scriptures/";

    private static final String CLASS_PARSER = "\\s+class=\"?";
    private static final String END_CLASS_PARSER = "[^\">]*\"?\\s*";
    private static final String END_TAG_PARSER = "[^>]*>";
    private static final String FOOTNOTES = "footnotes";

    private static final int SCRIPTURE_REFERENCE_HREF = 1;
    // private static final int SCRIPTURE_REFERENCE_BOOK = 2;
    // private static final int SCRIPTURE_REFERENCE_CHAPTER = 3;
    // private static final int SCRIPTURE_REFERENCE_VERSES = 4;
    private static final int SCRIPTURE_REFERENCE_ALT_VERSES = 5;
    // private static final int SCRIPTURE_REFERENCE_TARGET = 6;
    private static final int SCRIPTURE_REFERENCE_TEXT = 7;

    private static final String URL_BASE = "https://www.churchofjesuschrist.org";
    private static final String URL_ENSIGN = URL_BASE + "/study/magazines/ensign-19712020";
    private static final String URL_GENERAL_CONFERENCE = URL_BASE + "/study/general-conference";
    private static final String URL_LIAHONA = URL_BASE + "/study/magazines/liahona";
    private static final String URL_LANG = "?lang=";

    private static final String A_ACCENT = "(?:a|á|&#x[eE]1;|&#aacute;)";
    private static final String CAP_E_ACCENT = "(?:E|É|&#x[cC]9;|&#Eacute;)";
    private static final String E_ACCENT = "(?:e|é|&#x[eE]9;|&#eacute;)";
    private static final String I_ACCENT = "(?:i|í|&#x[eE][dD];|&#iacute;)";
    private static final String O_ACCENT = "(?:o|ó|&#x[fF]3;|&#oacute;)";
    private static final String U_ACCENT = "(?:u|ú|&#x[fF][aA];|&#uacute;)";
    private static final String SPACE = "(?:\\s| |&#x[aA]0;|&#160;)+";
    private static final String SPACE_OPTIONAL = "(?:\\s| |&#x[aA]0;|&#160;)?";

    // Properties
    private static final Logger logger = Logger.getLogger(SciCrawler.class.getName());

    private GregorianCalendar saturdayDate;
    private GregorianCalendar sundayDate;
    private int maxCitationId;

    private final String annual;
    private final String issueDate;
    private final Pattern jstPattern;
    private final String language;
    private final String monthString;
    private final Paths paths;
    private final Pattern referencePattern;
    private final Pattern scriptureReferencePattern;
    private final String sessionKey;
    private final int year;

    private final Database database = new Database();
    private final Speakers speakers = new Speakers(database);
    // private final Verses verses = new Verses();

    private final Map<String, int[]> pageRanges = new HashMap<>();
    private final List<String> sessions = new ArrayList<>();
    private final List<String> talkIds = new ArrayList<>();
    private final Map<String, String> talkHrefs = new HashMap<>();
    private final Map<String, Integer> talkSequence = new HashMap<>();
    private final Map<String, Integer> talkSessionNo = new HashMap<>();
    private final Map<String, String> talkSpeakers = new HashMap<>();
    private final Map<String, Integer> talkSpeakerIds = new HashMap<>();
    private final Map<String, String> talkSpeakerInitials = new HashMap<>();
    private final Map<String, String> talkTitles = new HashMap<>();
    private final Map<String, String> talkAudioUrls = new HashMap<>();
    private final Map<String, String[]> talkVideoUrls = new HashMap<>();
    private final Map<String, String> talkContents = new HashMap<>();
    private final Map<String, String> talkRewrittenContents = new HashMap<>();

    // Initialization
    public SciCrawler(int year, int month, String language) {
        this.year = year;
        this.language = language;
        String conferencePath;

        switch (year) {
            case 1971:
                // Published in June and December
                monthString = month == 4 ? "06" : "12";
                conferencePath = year + (month == 4 ? "apr" : "oct");
                break;
            case 1972:
            case 1973:
                // Published in July and January next year
                if (month == 4) {
                    monthString = "07";
                    conferencePath = year + "apr";
                } else {
                    monthString = "01";
                    conferencePath = (year + 1) + "oct";
                }
                break;
            default:
                // Published in May and November
                monthString = month == 4 ? "05" : "11";
                conferencePath = year + (month == 4 ? "apr" : "oct");
                break;
        }

        annual = month < 9 ? "A" : "S";
        issueDate = year + "-" + monthString + "-01";

        computeLikelyConferenceDates(year, month);

        paths = new Paths(conferencePath, language);

        String chapter = "(\\d+)";
        String verses = "(\\d+(?:[-–—,]" + SPACE_OPTIONAL + "\\d+)*)";

        if (language.equalsIgnoreCase("spa")) {
            String bibleBookName = "(G" + E_ACCENT + "nesis|" + CAP_E_ACCENT + "xodo"
                    + "|Lev" + I_ACCENT + "tico|N" + U_ACCENT + "meros"
                    + "|Deuteronomio|Josu" + E_ACCENT
                    + "|Jueces|Rut"
                    + "|(?:[12])" + SPACE
                    + "(?:Samuel|Reyes|Cr" + O_ACCENT + "nicas"
                    + "|Corintios|Tesalonicenses|Timoteo|Pedro|Juan)"
                    + "|3" + SPACE + "Juan"
                    + "|Esdras|Nehem" + I_ACCENT + "as|Ester|Job|Salmos?"
                    + "|Proverbios|Eclesiast" + E_ACCENT + "s|Cantares"
                    + "|Isa" + I_ACCENT + "as"
                    + "|Jerem" + I_ACCENT + "as|Lamentaciones|Ezequiel"
                    + "|Daniel|Oseas|Joel|Am" + O_ACCENT + "s"
                    + "|Abd" + I_ACCENT + "as|Jon" + A_ACCENT + "s|Miqueas"
                    + "|Nah" + U_ACCENT + "m|Habacuc|Sofon" + I_ACCENT + "as"
                    + "|Hageo|Zacar" + I_ACCENT + "as|Malaqu" + I_ACCENT + "as"
                    + "|Mateo|Marcos|Lucas|Juan|Hechos|Romanos"
                    + "|G" + A_ACCENT + "latas|Efesios|Filipenses|Colosenses"
                    + "|Tito|Filem" + O_ACCENT + "n|Hebreos|Santiago|Judas"
                    + "|Apocalipsis)";

            jstPattern = Pattern.compile(
                    "Traducci" + O_ACCENT + "n" + SPACE + "de" + SPACE
                            + "Jos" + E_ACCENT + SPACE + "Smith," + SPACE
                            + bibleBookName
                            + SPACE
                            + chapter
                            + ":"
                            + verses,
                    Pattern.MULTILINE | Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.CANON_EQ);

            sessionKey = "Sesión";
        } else {
            String bibleBookName = "(Genesis|Exodus|Leviticus|Numbers|Deuteronomy|Joshua" + "|Judges|Ruth" + "|(?:[12])"
                    + SPACE + "(?:Samuel|Kings|Chronicles|Corinthians" + "|Thessalonians|Timothy|Peter|John)" + "|3"
                    + SPACE + "John" + "|Ezra|Nehemiah|Esther|Job|Psalms?" + "|Proverbs|Ecclesiastes" + "|Song" + SPACE
                    + "of" + SPACE + "Solomon|Isaiah" + "|Jeremiah|Lamentations|Ezekiel|Daniel|Hosea|Joel|Amos"
                    + "|Obadiah|Jonah|Micah|Nahum|Habakkuk|Zephaniah|Haggai"
                    + "|Zechariah|Malachi|Matthew|Mark|Luke|John|Acts|Romans"
                    + "|Galatians|Ephesians|Philippians|Colossians|Titus"
                    + "|Philememon|Hebrews|James|Jude|Revelation)";

            jstPattern = Pattern.compile("Joseph" + SPACE + "Smith" + SPACE + "Translation," + SPACE + bibleBookName
                    + SPACE + chapter + ":" + verses, Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);

            sessionKey = "Session";
        }

        String hrefPattern = URL_SCRIPTURE_PATH
                + "(?:/study/scriptures/)?(?:ot|nt|bofm|dc-testament|pgp|jst)/"
                + "((?:jst-)?(?:gen|ex|lev|num|deut|josh|judg|ruth|1-sam"
                + "|2-sam|1-kgs|2-kgs|1-chr|2-chr|ezra|neh|esth|job|ps"
                + "|prov|eccl|song|isa|jer|lam|ezek|dan|hosea|joel|amos"
                + "|obad|jonah|micah|nahum|hab|zeph|hag|zech|mal|matt"
                + "|mark|luke|john|acts|rom|1-cor|2-cor|gal|eph|philip"
                + "|col|1-thes|2-thes|1-tim|2-tim|titus|philem|heb|james"
                + "|1-pet|2-pet|1-jn|2-jn|3-jn|jude|rev|bofm-title"
                + "|introduction|three|eight|1-ne|2-ne|jacob|enos|jarom"
                + "|omni|w-of-m|mosiah|alma|hel|3-ne|4-ne|morm|ether"
                + "|moro|dc|od|moses|abr|js-m|js-h|fac-1|fac-2|fac-3|a-of-f))"
                + "(?:(?:/([0-9]+))?[.]?(?:([0-9]+(?:[-,][0-9]+)*)"
                + "|study_intro[0-9])?)?" + "[?]lang=" + language
                + "(?:&amp;id=([^#]*))?(#p[A-Za-z0-9_-]+)?";

        referencePattern = Pattern.compile(hrefPattern);
        scriptureReferencePattern = Pattern
                .compile("<a\\s+class=\"scripture-ref\"\\s+href=\"(" + hrefPattern + ")\"[^>]*>(.*?)</a>");
    }

    // Static properties for command-line parameters

    static int requestedYear = -1;
    static int requestedMonth = -1;
    static String requestedLanguage = "";

    static boolean checkCitationConsistency = false;
    static boolean checkExternalLinks = false;
    static boolean invalidCommandLine = false;
    static boolean replaceTalkBodies = false;
    static boolean writeContentToFiles = false;
    static boolean writeToDatabase = false;
    static boolean useConferenceSite = false;

    public static void main(String[] args) {
        parseArguments(args);

        if (invalidCommandLine) {
            System.exit(-1);
        }

        if (args.length > 1) {
            checkYearMonthLanguage();
        }

        execute();
    }

    private static void checkCitationConsistency() {
        Database database = new Database();
        boolean foundInconsistency = false;

        Map<Integer, Boolean> citationIdsInTalks = database.citationIdsInTalks();
        Map<Integer, Boolean> citationIdsInTable = database.citationIdsInTable();

        for (Integer key : citationIdsInTalks.keySet()) {
            if (!citationIdsInTable.containsKey(key)) {
                logger.log(Level.WARNING, () -> "Citation ID " + key + " is in the talks but not in the table");
                foundInconsistency = true;
            }
        }

        for (Integer key : citationIdsInTable.keySet()) {
            if (!citationIdsInTalks.containsKey(key)) {
                logger.log(Level.WARNING, () -> "Citation ID " + key + " is in the table but not in the talks");
                foundInconsistency = true;
            }
        }

        if (!foundInconsistency) {
            logger.log(Level.INFO, () -> "All citation IDs are used consistently in talks and the citation table");
        }
    }

    private static void checkExternalLinks() {
        Database database = new Database();
        Map<Integer, List<String>> externalHrefs = database.externalLinksForTalks();
        Iterator<Map.Entry<Integer, List<String>>> iterator = externalHrefs.entrySet().iterator();
        boolean foundExternalLink = false;

        while (iterator.hasNext()) {
            Map.Entry<Integer, List<String>> entry = iterator.next();

            logger.log(Level.WARNING, () -> "Talk " + entry.getKey() + " has the following external links:");
            foundExternalLink = true;

            entry.getValue().forEach(href -> logger.log(Level.WARNING, () -> "    " + href));
        }

        if (!foundExternalLink) {
            logger.log(Level.INFO, () -> "No external links found in talks");
        }
    }

    private static void checkYearMonthLanguage() {
        if (requestedYear < 1971 || requestedMonth < 0 || (requestedMonth != 4 && requestedMonth != 10)
                || requestedYear > 2050
                || (!requestedLanguage.equals("eng") && !requestedLanguage.equals("spa")) || invalidCommandLine) {
            logger.log(Level.SEVERE, "Usage: SciCrawler year month language");
            logger.log(Level.SEVERE, "    where year is 1971 to 2050, month is either 4 or 10,");
            logger.log(Level.SEVERE, "    and language is either eng or spa");
            System.exit(-1);
        }
    }

    private static void crawl() {
        SciCrawler crawler = new SciCrawler(requestedYear, requestedMonth, requestedLanguage);

        crawler.getTableOfContents(useConferenceSite);
        crawler.readPageRanges();
        List<Link> scriptureCitations = crawler.crawlTalks(writeContentToFiles);
        crawler.checkSpeakers();
        crawler.writeCitationsToFile(scriptureCitations);
        crawler.showTables();
        crawler.processUpdatedCitations(scriptureCitations);

        crawler.updateDatabase(writeToDatabase, scriptureCitations);
        crawler.rewriteUrls(writeContentToFiles, scriptureCitations);
        crawler.updateTalkContents(writeToDatabase, replaceTalkBodies);
    }

    private static void execute() {
        if (checkCitationConsistency) {
            checkCitationConsistency();
        } else if (checkExternalLinks) {
            checkExternalLinks();
        } else {
            crawl();
        }
    }

    public static void parseArguments(String[] args) {
        if (args.length == 1) {
            if (args[0].equals("--checkCitationConsistency")) {
                checkCitationConsistency = true;
            } else if (args[0].equals("--checkExternalLinks")) {
                checkExternalLinks = true;
            } else {
                invalidCommandLine = true;
            }
        } else if (args.length >= 3) {
            int requiredArgCount = 0;

            for (int i = 0; i < args.length; i++) {
                if (args[i].startsWith("--")) {
                    switch (args[i]) {
                         case "--writeToDatabase":
                            writeToDatabase = true;
                            break;
                        case "--replaceTalkBodies":
                            replaceTalkBodies = true;
                            break;
                        case "--writeContentToFiles":
                            writeContentToFiles = true;
                            break;
                        case "--useConferenceSite":
                            useConferenceSite = true;
                            break;
                        default:
                            String badParameter = args[i];
                            logger.log(Level.WARNING, () -> "Unknown command-line parameter: " + badParameter);
                            invalidCommandLine = true;
                            break;
                    }
                } else {
                    try {
                        switch (requiredArgCount) {
                            case 0:
                                requestedYear = Integer.parseInt(args[i]);
                                break;
                            case 1:
                                requestedMonth = Integer.parseInt(args[i]);
                                break;
                            case 2:
                                requestedLanguage = args[i];
                                break;
                            default:
                                String badParameter = args[i];
                                logger.log(Level.WARNING, () -> "Too many command-line parameters: " + badParameter);
                                invalidCommandLine = true;
                                break;
                        }

                        requiredArgCount += 1;
                    } catch (NumberFormatException e) {
                        logger.log(Level.SEVERE, () -> "'" + args[0] + "' or '" + args[1] + "' is not an integer");
                        invalidCommandLine = true;
                    }
                }
            }
        } else {
            invalidCommandLine = true;
        }
    }

    /*
     * 1. Crawl talks.
     * 2. Extract citations.
     * 3. Manually verify/edit citations.
     * 4. Edit JSON to add new citations.
     * 5. Read update list and generate rewritten content.
     * 6. Update database (if flag set to write).
     */

    private void addFilteredLink(List<Link> links, String talkId, String href, String text, String footnoteKey) {
        String[] tags = { "scriptures/bd", "scriptures/gs", "scriptures/tg" };

        for (String tag : tags) {
            if (href.contains(tag)) {
                return;
            }
        }

        Link link = new Link(talkId, href, text, footnoteKey);

        link.addParsedToList(links, referencePattern);
    }

    private void addNonduplicateEntries(List<LinkEntry> newLinks, List<LinkEntry> existingLinks) {
        newLinks.forEach(link -> {
            if (!containsLinkEntry(existingLinks, link)) {
                existingLinks.add(link);
            }
        });
    }

    private void addNonduplicateLinks(List<Link> newLinks, List<Link> existingLinks) {
        newLinks.forEach(link -> {
            if (!containsLink(existingLinks, link)) {
                existingLinks.add(link);
            }
        });
    }

    private void checkSpeakers() {
        talkSpeakers.entrySet().forEach(entry -> {
            JSONObject speaker = speakers.matchingSpeaker(entry.getValue());

            if (speaker == null) {
                logger.log(Level.INFO, () -> "New speaker: " + entry.getValue());
                speakers.addSpeaker(entry.getValue());
            }
        });

        talkSpeakers.keySet().forEach(key -> {
            JSONObject speaker = speakers.matchingSpeaker(talkSpeakers.get(key));

            talkSpeakerIds.put(key, speaker.getInt("id"));
            talkSpeakerInitials.put(key, speaker.getString("abbr"));
        });
    }

    private String citationLink(LinkEntry entry, int citationId) {
        return "<span class=\"citation\" id=\"" + citationId + "\"><a href=\"javascript:void(0)\" onclick=\"sx(this, "
                + citationId + ")\">&nbsp;</a><a href=\"javascript:void(0)\" onclick=\"gs(" + citationId + ")\">"
                + entry.text + "</a></span>";
    }

    private int collectMatch(List<LinkEntry> entries, String content, Pattern pattern, boolean isFootnote,
            String footnoteKey, int sequence) {
        Matcher matcher = pattern.matcher(content);

        while (matcher.find()) {
            LinkEntry entry = new LinkEntry();

            entry.startIndex = matcher.start();
            entry.endIndex = matcher.end();
            entry.href = matcher.group(SCRIPTURE_REFERENCE_HREF);

            String verses = matcher.group(SCRIPTURE_REFERENCE_ALT_VERSES);

            if (verses != null && !verses.isEmpty()) {
                int queryIndex = entry.href.indexOf("?");

                verses = verses.replace("%2C", ",").replace("p", "");
                entry.href = entry.href.substring(0, queryIndex) + "." + verses + "?lang=" + language;
            }

            entry.text = matcher.group(SCRIPTURE_REFERENCE_TEXT);
            entry.isFootnote = isFootnote;
            entry.footnoteKey = matcher.group().contains("uniq") ? "uniq" : footnoteKey;
            entry.sequence = sequence;

            sequence += 1;

            entries.add(entry);
        }

        return sequence;
    }

    private void computeLikelyConferenceDates(int year, int month) {
        sundayDate = new GregorianCalendar(year, month - 1, 1);

        int dayOfWeek = sundayDate.get(Calendar.DAY_OF_WEEK);

        if (dayOfWeek > Calendar.SUNDAY) {
            sundayDate = new GregorianCalendar(year, month - 1, Calendar.SATURDAY - dayOfWeek + 2);
            saturdayDate = new GregorianCalendar(year, month - 1, Calendar.SATURDAY - dayOfWeek + 1);
        } else {
            saturdayDate = new GregorianCalendar(year, month - 2, month == 4 ? 31 : 30);
        }
    }

    private boolean containsLinkEntry(List<LinkEntry> links, LinkEntry link) {
        for (LinkEntry l : links) {
            if (link.isEqualTo(l)) {
                return true;
            }
        }

        return false;
    }

    private boolean containsLink(List<Link> links, Link link) {
        for (Link l : links) {
            if (link.isEqualTo(l)) {
                return true;
            }
        }

        return false;
    }

    private List<Link> crawlTalks(boolean writeContentToFiles) {
        List<Link> scriptureLinks = new ArrayList<>();

        File languageSubfolder = paths.languageDirectoryFile();

        if (!languageSubfolder.exists()) {
            languageSubfolder.mkdirs();
        }

        // For each talkId, get its contents JSON
        talkIds.forEach(talkId -> {
            logger.log(Level.INFO, () -> "Get " + talkId);

            File talkFile = paths.jsonTalkFile(talkId);
            JSONObject talkJson;

            if (!talkFile.exists()) {
                // Save talk if we haven't yet crawled it
                talkJson = new JSONObject(urlContent(talkUrl(talkId, language)));
                FileUtils.writeStringToFile(talkJson.toString(2), talkFile);
            } else {
                talkJson = editedTalkJson(talkId);
            }

            JSONObject talkContent = talkJson.getJSONObject("content");
            String talkBody = talkContent.getString("body");
            extractScriptureLinks(talkBody, talkId, scriptureLinks);

            String nonLinkedText = talkBody.replaceAll("<a[^>]*>.*?</a>", "");
            extractJstLinks(nonLinkedText, talkId, "body", scriptureLinks);

            if (talkContent.has(FOOTNOTES)) {
                JSONObject footnotes = talkContent.getJSONObject(FOOTNOTES);
                List<Link> footnoteLinks = new ArrayList<>();

                extractLinksFrom(footnotes, talkId, footnoteLinks);
                addNonduplicateLinks(footnoteLinks, scriptureLinks);
            }

            writeHtmlContent(writeContentToFiles, talkId, talkJson);
            extractAudioUrl(talkId, talkJson);
            extractVideoUrls(talkId, talkBody);
        });

        return scriptureLinks;
    }

    private String deactivateHyperlinks(StringBuilder builder) {
        Matcher matcher = Pattern.compile("<a\\s+class=\"cross-ref\"[^>]*>(.*?)</a>").matcher(builder);
        StringBuilder crossRefFiltered = new StringBuilder();

        if (matcher.find()) {
            crossRefFiltered.append(matcher.replaceAll(match -> match.group(1)));
        } else {
            crossRefFiltered = builder;
        }

        matcher = Pattern.compile("<a[^>]*scriptures/(?:bd|gs|tg)[^>]*>(.*?)</a>").matcher(crossRefFiltered);

        if (matcher.find()) {
            crossRefFiltered = new StringBuilder(matcher.replaceAll(match -> match.group(1)));
        }

        matcher = Pattern.compile("<a[^>]*href=\"http[^>]*>(.*?)</a>").matcher(crossRefFiltered);

        if (matcher.find()) {
            return matcher.replaceAll(match -> match.group(1));
        }

        return crossRefFiltered.toString();
    }

    private JSONObject editedTalkJson(String talkId) {
        File editedTalkFile = paths.editedJsonTalkFile(talkId);

        if (!editedTalkFile.exists()) {
            editedTalkFile = paths.jsonTalkFile(talkId);
        }

        return new JSONObject(FileUtils.stringFromFile(editedTalkFile));
    }

    private String editedSpeaker(String speaker) {
        if ((speaker.endsWith(" Jr.") || speaker.endsWith(" Sr.")) && !speaker.contains(", ")) {
            return speaker.substring(0, speaker.length() - 4) + ","
                    + speaker.substring(speaker.length() - 4);
        }

        return speaker;
    }

    private void extractAudioUrl(String talkId, JSONObject talkJson) {
        JSONObject meta = talkJson.getJSONObject("meta");

        if (meta != null) {
            JSONArray audio = meta.getJSONArray("audio");

            if (audio != null && !audio.isEmpty()) {
                JSONObject audioObject = (JSONObject) audio.get(0);

                if (audioObject != null) {
                    String mediaUrl = audioObject.getString("mediaUrl");

                    if (mediaUrl != null) {
                        talkAudioUrls.put(talkId, mediaUrl);
                    }
                }
            }
        }
    }

    private void extractJstLinks(String content, String talkId, String footnoteKey, List<Link> links) {
        Matcher matcher = jstPattern.matcher(content);

        while (matcher.find()) {
            String fullMatch = matcher.group(0);
            String book = matcher.group(1);
            String chapter = matcher.group(2);
            String verses = matcher.group(3).replaceAll(SPACE, "");
            String bookAbbr = BookFinder.sInstance.abbreviationForBook(book);
            String volume = "jst"; // BookFinder.sInstance.volumeForBook(book);

            if (!bookAbbr.startsWith("jst-")) {
                bookAbbr = "jst-" + bookAbbr;
            }

            links.add(new Link(
                    talkId, URL_SCRIPTURE_PATH + volume + PATH_SEPARATOR
                            + bookAbbr + PATH_SEPARATOR + chapter + "." + verses + URL_LANG
                            + language /* + "#p" + Link.firstVerse(verses) */,
                    fullMatch, bookAbbr, chapter, verses, true, footnoteKey));
        }
    }

    private int footnoteIndex = 0;

    private String hrefRewrittenForAltVerses(String href) {
        String ampersandString = "&amp;id=";
        int queryIndex = href.indexOf("?");
        int ampersandIndex = href.indexOf(ampersandString, queryIndex);
        int hashIndex = href.indexOf("#", ampersandIndex);

        if (queryIndex > 0 && ampersandIndex > 0 && hashIndex > 0) {
            String verses = href.substring(ampersandIndex + ampersandString.length(), hashIndex)
                .replace("%2C", ",")
                .replace("p", "");

            href = href.substring(0, queryIndex) + "." + verses + "?lang=" + language;

            return href;
        }

        return href;
    }

    private void extractLinksFrom(JSONObject footnotes, String talkId, List<Link> links) {
        String[] keys = new String[footnotes.keySet().size()];

        footnotes.keySet().toArray(keys);
        Arrays.sort(keys, new SortByNote());

        for (String key : keys) {
            JSONObject footnote = footnotes.getJSONObject(key);

            if (footnote.has("referenceUris")) {
                JSONArray uris = footnote.getJSONArray("referenceUris");

                uris.forEach(subitem -> {
                    JSONObject uri = (JSONObject) subitem;

                    footnoteIndex += 1;

                    if (uri.has("href") && uri.has("type") && uri.has("text")
                            && uri.getString("type").equals("scripture-ref")) {
                        addFilteredLink(
                            links,
                            talkId,
                            hrefRewrittenForAltVerses(uri.getString("href")),
                            uri.getString("text"),
                            key + "-" + footnoteIndex
                        );
                    }
                });
            }

            List<Link> jstLinks = new ArrayList<>();

            extractJstLinks(footnote.getString("text").replace("%2C", ","), talkId, key, jstLinks);

            if (!jstLinks.isEmpty()) {
                addNonduplicateLinks(jstLinks, links);
            }
        }
    }

    private String extractMatch(String text, String regex) {
        Matcher matcher = Pattern.compile(regex, Pattern.MULTILINE | Pattern.DOTALL).matcher(text);

        if (matcher.find()) {
            return matcher.group(1);
        }

        return null;
    }

    private void extractScriptureLinks(String content, String talkId, List<Link> links) {
        Matcher matcher = scriptureReferencePattern.matcher(content);

        while (matcher.find()) {
            // NOTE: if the same scripture appears twice, once in the body and once in the
            // footnotes, then we need to mark it as unique so the two can be distinguished.
            // Add "uniq" to the end of the <a> tag as in D&C 42:12 in 56bednar April 2021.
            String key = matcher.group().contains("uniq") ? "uniq" : "body";
            String href = matcher.group(SCRIPTURE_REFERENCE_HREF);
            String verses = matcher.group(SCRIPTURE_REFERENCE_ALT_VERSES);
            String citationText = matcher.group(SCRIPTURE_REFERENCE_TEXT);

            if (verses != null && !verses.isEmpty()) {
                verses = verses.replace("%2C", ",").replace("p", "");
                int queryIndex = href.indexOf("?");

                href = href.substring(0, queryIndex) + "." + verses + "?lang=" + language;
            }

            addFilteredLink(links, talkId, href, citationText, key);
        }
    }

    private void extractVideoUrls(String talkId, String content) {
        List<String> urls = new ArrayList<>();
        int index1 = content.indexOf("<video");

        if (index1 < 0) {
            return;
        }

        int index2 = content.indexOf("</video>", index1);

        if (index2 < 0) {
            return;
        }

        Matcher matcher = Pattern.compile("<source\\s+src=\"([^\"]*)\"\\s+type=\"([^\"]*)\"")
                .matcher(content.substring(index1, index2));

        while (matcher.find()) {
            String type = matcher.group(2);

            if (type.equals("video/mp4")) {
                urls.add(matcher.group(1));
            }
        }

        if (!urls.isEmpty()) {
            talkVideoUrls.put(talkId, urls.toArray(String[]::new));
        }
    }

    private int filterTalkSubitem(String itemHref, String itemTitle, String itemSpeaker, int sessionNumber,
            int talkNumber) {
        String itemId = extractMatch(itemHref, "[0-9]+/[0-9]+/(.*)\\?");

        if (itemTitle.contains("Auditing Department")
                || itemTitle.contains("Sustaining of General Authorities")
                || itemTitle.contains("Departamento de Auditorías")
                || itemTitle.contains("Sostenimiento de las Autoridades Generales")
                || itemTitle.startsWith("Video:")) {
            return talkNumber;
        }

        ++talkNumber;

        talkIds.add(itemId);
        talkHrefs.put(itemId, itemHref);
        talkTitles.put(itemId, itemTitle);
        talkSpeakers.put(itemId, editedSpeaker(itemSpeaker));
        talkSequence.put(itemId, talkNumber);
        talkSessionNo.put(itemId, sessionNumber);

        final int talkNumberToLog = talkNumber;
        logger.log(Level.INFO, () -> itemId
                + "\t" + talkNumberToLog
                + "\t" + sessionNumber
                + "\t" + StringUtils.encodeSpecialCharacters(itemTitle)
                + "\t" + itemSpeaker);

        return talkNumber;
    }

    private void formatFootnotes(StringBuilder talk, JSONObject footnotes) {
        /*
         * "note4": {
         * "marker": "4.",
         * "context": null,
         * "pid": "143139345",
         * "id": "note4",
         * "text": "<p data-aid=\"143139369\" id=\"note4_p1\">See
         * <a class=
         * \"scripture-ref\" href=\"/study/scriptures/dc-testament/dc/8.2-3?lang=eng#p2\"
         * >Doctrine
         * and Covenants 8:2&#x2013;3<\/a>.<\/p>",
         * "referenceUris": [{
         * "href": "/study/scriptures/dc-testament/dc/8.2-3?lang=eng#p2",
         * "text": "Doctrine and Covenants 8:2\u20133",
         * "type": "scripture-ref"
         * }]
         * },
         */

        talk.append("\n<footer class=\"notes\">\n");
        talk.append("<p class=\"title\" id=\"note_title1\">");
        talk.append(language.equals("spa") ? "Notas" : "Notes");
        talk.append("</p>\n");
        talk.append("<ol class=\"decimal\" data-type=\"marker\" start=\"1\">\n");

        for (String key : sortedKeys(footnotes)) {
            JSONObject footnote = footnotes.getJSONObject(key);

            talk.append("<li data-marker\"" + footnote.getString("marker")
                    + "\" id=\"" + footnote.getString("id") + "\">\n");
            talk.append(footnote.getString("text"));
            talk.append("</li>\n");
        }

        talk.append("</ol></footer>\n");
    }

    private void getTableOfContents(boolean useConferenceSite) {
        String content = tableOfContentsHtml(useConferenceSite);

        if (content != null) {
            String subitems = talkSubitemsFromTableOfContents(content);

            parseTalkSubitems(subitems);
        }
    }

    private String inlinedFootnote(String noteKey, JSONObject footnotes) {
        JSONObject footnote = footnotes.getJSONObject("note" + noteKey);
        String footnoteText = footnote.getString("text")
                .replaceAll("<p[^>]*>", "<span class=\"note-p\">")
                .replace("</p>", "</span>");
        boolean textIsLong = visibleLength(footnoteText) > 40;

        return "<sup class=\"noteMarker\"><a href=\"#note" + noteKey + "\">" + noteKey + "</a><span class=\"footnote"
                + (textIsLong ? " long" : "") + "\">[" + footnoteText + "]</span></sup>";
    }

    private void inlineFootnotes(StringBuilder talk, JSONObject footnotes) {
        //
        // <a class="note-ref" href="#note1"><sup class="marker" data-value="1"></sup></a>
        //
        String markerParser = "<a\\s*class=\"note-ref\"\\s*href=\"#note(\\d+)\">"
                + "\\s*<sup\\s*class=\"marker\"\\s*(?:data-value=\"(\\d+)\")?>\\s*(\\d+)?\\s*</sup>"
                + "\\s*</a>";
        Matcher markerMatcher = Pattern.compile(markerParser).matcher(talk);
        List<FootnoteMatch> matches = new ArrayList<>();

        while (markerMatcher.find()) {
            matches.add(new FootnoteMatch(markerMatcher));
        }

        for (int i = matches.size() - 1; i >= 0; i--) {
            FootnoteMatch match = matches.get(i);
            talk.replace(match.start, match.end, inlinedFootnote(match.matchText, footnotes));
        }

        // while (markerMatcher.find()) {
        // talk.replace(
        // markerMatcher.start(),
        // markerMatcher.end(),
        // inlinedFootnote(markerMatcher.group(1), footnotes)
        // );
        // }
    }

    private String magazineForLanguageYear(String language) {
        if (year <= 2020) {
            return language.equals("eng") ? "ensign" : "liahona";
        }

        return "general-conference";
    }

    private String monthStringForYearMonth(int year, String month) {
        if (year <= 2020) {
            return month;
        }

        return month.equals("11") ? "10" : "04";
    }

    private int parseSessionItems(String sessionItems, int sessionNumber, int talkNumber) {
        String listItemParser = "<li>(.*?)<\\/li>";
        String itemParser = "<a" + CLASS_PARSER + "item" + END_CLASS_PARSER + "href=\"([^\"]+)\""
                + END_TAG_PARSER + "\\s*<div" + CLASS_PARSER + "itemTitle" + END_CLASS_PARSER + END_TAG_PARSER
                + "(?:<span\\s+class=activeMarker[^>]*>\\s*<\\/span>)?"
                + "\\s*<p>\\s*<span>\\s*(.*?)\\s*<\\/span>\\s*<\\/p>\\s*" + "<p" + CLASS_PARSER + "subtitle"
                + END_CLASS_PARSER + END_TAG_PARSER + "\\s*([^<]*)\\s*<\\/p>" + "\\s*<\\/div>\\s*<\\/a>";

        Matcher listItemMatcher = Pattern.compile(listItemParser).matcher(sessionItems.replace("\u00A0", " "));
        Pattern itemPattern = Pattern.compile(itemParser);

        while (listItemMatcher.find()) {
            Matcher detailMatcher = itemPattern.matcher(listItemMatcher.group(1));

            while (detailMatcher.find()) {
                talkNumber = filterTalkSubitem(
                        detailMatcher.group(1),
                        detailMatcher.group(2),
                        detailMatcher.group(3),
                        sessionNumber,
                        talkNumber);
            }
        }

        return talkNumber;
    }

    private void parseTalkSubitems(String subitems) {
        int sessionNumber = 0;
        int talkNumber = 0;
        String subitemParser = "<li>\\s*<(?:a|span)" + CLASS_PARSER + "sectionTitle" + END_CLASS_PARSER + END_TAG_PARSER
                + "\\s*<div" + CLASS_PARSER + "itemTitle" + END_CLASS_PARSER + END_TAG_PARSER
                + "(?:<span\\s+class=activeMarker[^>]*>\\s*<\\/span>)?"
                + "\\s*<p>\\s*<span>\\s*(.*?)\\s*<\\/span>\\s*<\\/p>\\s*" + "\\s*<\\/div>\\s*<\\/(?:a|span)>\\s*"
                + "(?:" + "\\s*<ul" + CLASS_PARSER + "subItems" + END_CLASS_PARSER + END_TAG_PARSER + "\\s*(.*?)"
                + "\\s*<\\/ul>" + ")?\\s*<\\/li>";
        Matcher matcher = Pattern.compile(subitemParser, Pattern.DOTALL).matcher(subitems);

        while (matcher.find()) {
            String sessionTitle = matcher.group(1).trim();
            String sessionItems = matcher.group(2);

            if (sessionTitle.contains(sessionKey)) {
                sessions.add(sessionTitle);
                ++sessionNumber;
                talkNumber = parseSessionItems(sessionItems, sessionNumber, talkNumber);
            }
        }
    }

    private void processUpdatedCitations(List<Link> citations) {
        File citationsFile = paths.updatedCitationsFile();

        if (citationsFile.exists()) {
            String[] lines = Arrays.stream(FileUtils.stringFromFile(citationsFile).split("\n"))
                    .filter(line -> !line.trim().startsWith("#")).toArray(String[]::new);

            for (int i = 0; i < lines.length; i++) {
                String[] fields = lines[i].split("\t");
                Link link = citations.get(i);

                if (fields[0].equalsIgnoreCase("DELETE")) {
                    if (fields.length != 5 || !link.href.contains(fields[1])) {
                        final int line = i + 1;
                        logger.log(Level.SEVERE, () -> "Incorrect DELETE line at line " + line);
                        System.exit(-1);
                        return;
                    }

                    link.isDeleted = true;
                } else if (fields[0].equalsIgnoreCase("ADD")) {
                    if (fields.length != 6) {
                        final int line = i + 1;
                        logger.log(Level.SEVERE, () -> "Incorrect ADD line format at line " + line);
                        System.exit(-1);
                        return;
                    }

                    // Expect format ADD href initials page talkId text
                    citations.add(i, new Link(fields[1], fields[3], fields[4], fields[5], language, referencePattern));
                    logger.log(Level.INFO, () -> "Ensure you have edited talk " + fields[4] + " to insert link");
                } else {
                    if (fields.length != 4 || !link.href.contains(fields[0])) {
                        final int line = i + 1;
                        final String lineText = lines[i];
                        logger.log(Level.SEVERE, () -> "Incorrect line format or field mismatch at line " + line);
                        logger.log(Level.SEVERE, () -> "Line: [" + lineText + "]");
                        logger.log(Level.SEVERE, () -> "Link is " + link.text + ", " + link.href);
                        logger.log(Level.SEVERE, () -> "fields[0] [" + fields[0] + "] is not in link.href");
                        System.exit(-1);
                        return;
                    }

                    if (!link.href.contains(fields[0])) {
                        final int line = i + 1;
                        logger.log(Level.SEVERE, () -> "Misaligned input citation at line " + line);
                        System.exit(-1);
                        return;
                    }

                    link.page = Utils.integerValue(fields[2]);
                }
            }

            if (lines.length != citations.size()) {
                logger.log(Level.SEVERE, "Unexpected size for updated citations file");
                System.exit(-1);
            }
        } else {
            logger.log(Level.WARNING, () -> "Need updated citations file (" + paths.updatedCitationsFile() + ")");
        }
    }

    private void readPageRanges() {
        String maxVerseData = FileUtils.stringFromFile(paths.pagesFile());
        String[] lines = maxVerseData.split("\n");

        for (String line : lines) {
            String[] columns = line.split("\t");

            if (columns.length == 3) {
                String talkId = columns[0];
                int startPage = Integer.parseInt(columns[1]);
                int endPage = Integer.parseInt(columns[2]);
                int[] pageRange = new int[] { startPage, endPage };

                pageRanges.put(talkId, pageRange);
            }
        }
    }

    private void rewriteUrls(boolean writeContentToFiles, List<Link> citations) {
        maxCitationId = database.getMaxCitationId() + 1;

        File rewrittenDirFile = paths.rewrittenDirectoryFile();

        if (!rewrittenDirFile.exists()) {
            rewrittenDirFile.mkdirs();
        }

        for (String talkId : talkIds) {
            logger.log(Level.INFO, () -> "Rewriting URLs for " + talkId);
            File rewrittenTalkFile = paths.rewrittenTalkFile(talkId);
            JSONObject talkJson = editedTalkJson(talkId);
            Link[] talkCitations = citations.stream().filter(citation -> citation.talkId.equals(talkId))
                    .toArray(Link[]::new);
            String content = talkHtmlContentForJson(talkId, talkJson, true, talkCitations);

            talkRewrittenContents.put(talkId, content);

            if (writeContentToFiles) {
                FileUtils.writeStringToFile(content, rewrittenTalkFile);
            }
        }
    }

    private String rewriteUrlsForTalkId(String talkId, StringBuilder body, JSONObject footnotes, Link[] citations) {
        List<LinkEntry> entries = new ArrayList<>();
        int citationCount = 0;
        int sequence = 0;

        String talkBody = Pattern.compile("<footer\\s*class=\"notes\">.*?</footer>",
                Pattern.DOTALL + Pattern.MULTILINE)
                .matcher(body).replaceFirst("");

        collectMatch(entries, talkBody, scriptureReferencePattern, false, "body", 0);

        if (footnotes != null) {
            for (String key : sortedKeys(footnotes)) {
                JSONObject footnote = footnotes.getJSONObject(key);
                List<LinkEntry> footnoteEntries = new ArrayList<>();

                sequence = collectMatch(footnoteEntries, footnote.getString("text"), scriptureReferencePattern, true,
                        key, sequence);
                addNonduplicateEntries(footnoteEntries, entries);
            }
        }

        if (entries.size() != citations.length) {
            findMismatchedHyperlinkEntry(entries, citations, talkId);
        } else {
            for (int i = entries.size() - 1; i >= 0; i--) {
                LinkEntry entry = entries.get(i);
                Link citation = citations[i];
                int citationId = citation.citationId;

                if (citationId <= 0) {
                    citationId = maxCitationId + i;
                }

                if (entry.isFootnote && footnotes != null) {
                    JSONObject footnote = footnotes.getJSONObject(entry.footnoteKey);
                    StringBuilder footnoteText = new StringBuilder(footnote.getString("text"));

                    footnote.put("text",
                            footnoteText
                                    .replace(entry.startIndex, entry.endIndex,
                                            citation.isDeleted ? "" : citationLink(entry, citationId))
                                    .toString());
                } else {
                    body.replace(entry.startIndex, entry.endIndex,
                            citation.isDeleted ? "" : citationLink(entry, citationId));
                }

                if (!citation.isDeleted) {
                    citationCount += 1;
                }
            }

            maxCitationId += citationCount;
        }

        if (footnotes != null) {
            inlineFootnotes(body, footnotes);
        }

        return body.toString();
    }

    private void findMismatchedHyperlinkEntry(List<LinkEntry> entries, Link[] citations, String talkId) {
        for (int i = 0; i < entries.size() && i < citations.length; i++) {
            LinkEntry entry = entries.get(i);
            Link link = citations[i];

            if (!entry.href.equalsIgnoreCase(link.href)) {
                final int index = i;

                logger.log(Level.SEVERE, () -> "First mismatch at position "
                        + index + " for " + entry.href + " link href " + link.href);
                break;
            }
        }

        StringBuilder entriesText = new StringBuilder("Hyperlink list:\n");

        entries.forEach(entry -> {
            entriesText.append(entry.href);
            entriesText.append("\n");
        });

        entriesText.append("\nCitations list:\n");

        for (int i = 0; i < citations.length; i++) {
            entriesText.append(citations[i].href);
            entriesText.append("\n");
        }

        logger.log(Level.SEVERE, entriesText::toString);
        logger.log(Level.SEVERE, () -> "Hyperlink list size doesn't match citations list size");
        logger.log(Level.SEVERE, () -> "Correct this mismatch in " + talkId + " before proceeding");
        System.exit(-1);
    }

    private void showTables() {
        talkIds.forEach(talkId -> {
            int[] pageRange = pageRanges.get(talkId);

            logger.log(Level.INFO, () -> talkId + "|" + talkHrefs.get(talkId) + "|" + talkTitles.get(talkId) + "|"
                    + talkSpeakers.get(talkId) + "|" + pageRange[0] + "|" + pageRange[1]);
        });

        sessions.forEach(session -> logger.log(Level.INFO, session));

        logger.log(Level.INFO, () -> "Saturday: " + year + "-" + (saturdayDate.get(Calendar.MONTH) + 1) + "-"
                + saturdayDate.get(Calendar.DAY_OF_MONTH));
        logger.log(Level.INFO, () -> "Sunday: " + year + "-" + (sundayDate.get(Calendar.MONTH) + 1) + "-"
                + sundayDate.get(Calendar.DAY_OF_MONTH));

        logger.log(Level.INFO, () -> year + (annual.equals("A") ? " Annual" : " Semi-Annual") + " General Conference"
                + ", " + issueDate);
    }

    private List<String> sortedKeys(JSONObject footnotes) {
        List<String> keys = new ArrayList<>(footnotes.keySet().size());

        for (String key : footnotes.keySet()) {
            keys.add(key);
        }

        keys.sort((k1, k2) -> {
            int i1 = Integer.parseInt(k1.replaceAll("\\D*", ""));
            int i2 = Integer.parseInt(k2.replaceAll("\\D*", ""));

            return i1 - i2;
        });

        return keys;
    }

    private String tableOfContentsHtml(boolean useConferenceSite) {
        File conferenceDirectoryFile = paths.conferenceDirectoryFile();
        File cachedContentFile = paths.cachedContentFile(language);
        String content;

        if (!conferenceDirectoryFile.exists()) {
            conferenceDirectoryFile.mkdir();
        }

        if (cachedContentFile.exists()) {
            content = FileUtils.stringFromFile(cachedContentFile);
        } else {
            content = urlContent(urlForConferenceContents(useConferenceSite));
            FileUtils.writeStringToFile(content, cachedContentFile);
        }

        return content;
    }

    private String talkHtmlContentForJson(String talkId, JSONObject talkJson, boolean inlineFootnotes,
            Link[] citationsToRewrite) {
        StringBuilder talk = new StringBuilder();

        JSONObject talkContent = talkJson.getJSONObject("content");
        String talkBody = talkContent.getString("body");
        JSONObject footnotes = null;

        if (talkContent.has(FOOTNOTES)) {
            footnotes = talkContent.getJSONObject(FOOTNOTES);
        }

        talkBody = talkBody
                .replaceAll("<link[^>]*>", "")
                .replaceAll("<video[^>]*>.*<\\/video>", "")
                .replaceAll("<img[^>]*>", "");
        talkBody = Pattern.compile("<footer\\s*class=\"notes\">.*?</footer>", Pattern.DOTALL + Pattern.MULTILINE)
                .matcher(talkBody).replaceFirst("");
        talkBody = Pattern.compile("<figure\\s*class=\"[^\"]*no-print.*?</figure>", Pattern.DOTALL + Pattern.MULTILINE)
                .matcher(talkBody).replaceAll("");

        talk.append("<div class=\"body\">");
        talk.append(talkBody);

        if (footnotes != null) {
            if (inlineFootnotes) {
                rewriteUrlsForTalkId(talkId, talk, footnotes, citationsToRewrite);
            }

            if (!footnotes.keySet().isEmpty() && talkBody.indexOf("<footer") < 0) {
                formatFootnotes(talk, footnotes);
            }
        }

        talk.append("</div>");

        return StringUtils.encodeSpecialCharacters(deactivateHyperlinks(talk));
    }

    private String talkSubitemsFromTableOfContents(String content) {
        // Extract the subitems <ul> from the document
        String contentParser = "<nav" + CLASS_PARSER + "tableOfContents"
                + END_CLASS_PARSER + END_TAG_PARSER
                + "\\s*<(?:a|span)" + CLASS_PARSER + "bookTitle" + END_CLASS_PARSER + END_TAG_PARSER
                + ".*?</(?:a|span)>"
                + "(.*?)<\\/nav>";

        return extractMatch(
                extractMatch(content, contentParser),
                "<ul" + END_TAG_PARSER + "(.*)</ul>");
    }

    private String talkUrl(String talkId, String language) {
        return URL_BASE + "/study/api/v3/language-pages/type/content?lang="
                + language + "&uri=%2F"
                + magazineForLanguageYear(language)
                + "%2F" + year + "%2F"
                + monthStringForYearMonth(year, monthString)
                + "%2F" + talkId;
    }

    private void updateDatabase(boolean writeToDatabase, List<Link> citations) {
        database.updateSpeakers(writeToDatabase, speakers);
        database.updateMetaData(writeToDatabase, year, language, annual, issueDate, sessions, saturdayDate, sundayDate,
                talkIds,
                talkHrefs, talkSpeakerIds, talkTitles, pageRanges, talkSequence, talkSessionNo, talkAudioUrls,
                talkVideoUrls);
        database.updateCitations(writeToDatabase, citations, talkIds, year, annual, language);
    }

    private void updateTalkContents(boolean writeToDatabase, boolean replaceTalkBodies) {
        database.updateTalkContents(writeToDatabase, replaceTalkBodies, talkRewrittenContents, language);
    }

    private String urlContent(String url) {
        String content = null;
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);

        httpGet.addHeader("User-Agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.11 Safari/537.36");
        httpGet.addHeader("Accept-Charset", "utf-8");

        try {
            CloseableHttpResponse httpResponse = httpClient.execute(httpGet);

            content = new String(httpResponse.getEntity().getContent().readAllBytes(),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.log(Level.SEVERE, () -> "Unable to retrive URL " + url);
            System.exit(-1);
        }

        httpGet.releaseConnection();
        try {
            httpClient.close();
        } catch (IOException e) {
            // Ignore failure
        }

        return content;
    }

    private String urlForConferenceContents(boolean useConferenceSite) {
        if (useConferenceSite || language.equals("spa")) {
            String urlMonth;

            if (monthString.equals("05")) {
                urlMonth = "04";
            } else {
                urlMonth = "10";
            }

            return URL_GENERAL_CONFERENCE + "/" + year + "/" + urlMonth + URL_LANG + language;
        }

        return ((year < 2021) ? URL_ENSIGN : URL_LIAHONA) + "/" + year + "/" + monthString + URL_LANG + language;
    }

    private int visibleLength(String html) {
        StringBuilder visibleText = new StringBuilder(html);

        SearchTerm.replaceAll("<[^>]+>", visibleText);
        SearchTerm.replaceAll("&nbsp;", visibleText);
        String reducedText = visibleText.toString().trim().replaceAll("\\s\\s+", " ");

        return reducedText.length();
    }

    private void writeCitationsToFile(List<Link> scriptureLinks) {
        StringBuilder citations = new StringBuilder();

        for (int i = 0; i < scriptureLinks.size(); i++) {
            Link link = scriptureLinks.get(i);
            final int index = i + 1;
            int ix1 = link.href.indexOf("/", 18);
            int ix2 = link.href.indexOf("?", ix1);

            logger.log(Level.INFO, () -> "link href " + link.href);
            citations.append(link.href.substring(ix1 + 1, ix2));
            citations.append("\t");
            citations.append(talkSpeakerInitials.get(link.talkId));
            citations.append("\t");

            citations.append(pageRanges.get(link.talkId)[1]);
            citations.append("\t");
            citations.append(link.talkId);
            citations.append("\n");

            logger.log(Level.INFO, () -> index + "\t" + link.talkId + "\t" + link.href + "\t" + link.text + "\t"
                    + link.book + "\t" + link.chapter + "\t" + link.verses + "\t" + link.isJst);
        }

        FileUtils.writeStringToFile(citations.toString(), paths.citationsFile());
    }

    private void writeHtmlContent(boolean writeContentToFiles, String talkId, JSONObject talkJson) {
        String content = talkHtmlContentForJson(talkId, talkJson, false, null);

        talkContents.put(talkId, content);

        if (writeContentToFiles) {
            FileUtils.writeStringToFile(content, paths.talkFile(talkId));
        }
    }

    private class FootnoteMatch {
        int start;
        int end;
        String matchText;

        FootnoteMatch(Matcher matcher) {
            start = matcher.start();
            end = matcher.end();
            matchText = matcher.group(1);
        }
    }

    private class LinkEntry {
        int startIndex;
        int endIndex;
        int sequence;
        String href;
        String text;
        boolean isFootnote = false;
        String footnoteKey;

        boolean isEqualTo(LinkEntry l) {
            return href.equals(l.href)
                    && text.equals(l.text)
                    && sequence == l.sequence
                    && (footnoteKey.equals(l.footnoteKey) || footnoteKey.equals("body")
                            || l.footnoteKey.equals("body"));
        }
    }
}
