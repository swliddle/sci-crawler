package edu.byu.sci.crawler;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONObject;

public class SciCrawler {

    // Constants

    private static final String CLASS_PARSER = "\\s+class=\"?";
    private static final String END_CLASS_PARSER = "[^\">]*\"?\\s*";
    private static final String END_TAG_PARSER = "[^>]*>";

    private static final String HYPHEN = "-";
    private static final String NDASH = "–";

    private static final String URL_BASE = "https://www.churchofjesuschrist.org";
    private static final String URL_ENSIGN = URL_BASE + "/study/ensign";
    private static final String URL_LIAHONA = URL_BASE + "/study/liahona";

    private static final String WITH_DELIMITER = "((?<=%1$s)|(?=%1$s))";

    // Properties

    private final String monthString;
    private final int year;
    private final String conferencePath;
    private final Pattern jstPattern;
    private final String language;
    private final Pattern referencePattern;

    private final Books books = new Books();

    private final List<String> talkIds = new ArrayList<>();
    private final Map<String, String> talkHrefs = new HashMap<>();
    private final Map<String, String> talkSpeakers = new HashMap<>();
    private final Map<String, String> talkTitles = new HashMap<>();

    // Initialization

    public SciCrawler(int year, int month, String language) {
        this.year = year;
        this.language = language;

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

        String chapter = "([0-9]+)";
        String verses = "([0-9]+((?:[-,])[0-9]+)*)";
        String space = "(?:\\s| |&#x[aA]0;|&#160;)+";

        if (language.equalsIgnoreCase("spa")) {
            String aAccent = "(?:a|á|&#x[eE]1;|&#aacute;)";
            String eAccent = "(?:e|é|&#x[eE]9;|&#eacute;)";
            String EAccent = "(?:E|É|&#x[cC]9;|&#Eacute;)";
            String iAccent = "(?:i|í|&#x[eE][dD];|&#iacute;)";
            String oAccent = "(?:o|ó|&#x[fF]3;|&#oacute;)";
            String uAccent = "(?:u|ú|&#x[fF][aA];|&#uacute;)";
            String bibleBookName =
                    "(G" + eAccent + "nesis|" + EAccent + "xodo"
                    + "|Lev" + iAccent + "tico|N" + uAccent + "meros"
                    + "|Deuteronomio|Josu" + eAccent
                    + "|Jueces|Rut"
                    + "|(?:1|2)" + space
                    +     "(?:Samuel|Reyes|Cr" + oAccent + "nicas"
                    +      "|Corintios|Tesalonicenses|Timoteo|Pedro|Juan)"
                    + "|3" + space + "Juan"
                    + "|Esdras|Nehem" + iAccent + "as|Ester|Job|Salmos?"
                    + "|Proverbios|Eclesiast" + eAccent + "s|Cantares"
                    + "|Isa" + iAccent + "as"
                    + "|Jerem" + iAccent + "as|Lamentaciones|Ezequiel"
                    + "|Daniel|Oseas|Joel|Am" + oAccent + "s"
                    + "|Abd" + iAccent + "as|Jon" + aAccent + "s|Miqueas"
                    + "|Nah" + uAccent + "m|Habacuc|Sofon" + iAccent + "as"
                    + "|Hageo|Zacar" + iAccent + "as|Malaqu" + iAccent + "as"
                    + "|Mateo|Marcos|Lucas|Juan|Hechos|Romanos"
                    + "|G" + aAccent + "latas|Efesios|Filipenses|Colosenses"
                    + "|Tito|Filem" + oAccent + "n|Hebreos|Santiago|Judas"
                    + "|Apocalipsis)";

            jstPattern = Pattern.compile(
                    "Traducci" + oAccent + "n" + space + "de" + space
                        + "Jos" + eAccent + space + "Smith," + space
                        + bibleBookName
                        + space
                        + chapter
                        + ":"
                        + verses,
                    Pattern.MULTILINE | Pattern.CASE_INSENSITIVE
            );
            referencePattern = Pattern.compile(
                    "\\/study\\/scriptures\\/"
                    + "(?:ot|nt|bofm|dc-testament|pgp|jst)\\/"
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
                    + "(?:(?:\\/([0-9]+))?[.]?(?:([0-9]+(?:[-,][0-9]+)*)"
                    + "|study_intro[0-9])?)?"
                    + "[?]lang=" + language + "(#p[A-Za-z0-9_-]+)?"
            );
        } else {
            String bibleBookName =
                    "(Genesis|Exodus|Leviticus|Numbers|Deuteronomy|Joshua"
                    + "|Judges|Ruth"
                    + "|(?:1|2)" + space
                    +     "(?:Samuel|Kings|Chronicles|Corinthians"
                    +       "|Thessalonians|Timothy|Peter|John)"
                    + "|3" + space + "John"
                    + "|Ezra|Nehemiah|Esther|Job|Psalms?"
                    + "|Proverbs|Ecclesiastes"
                    + "|Song" + space + "of" + space + "Solomon|Isaiah"
                    + "|Jeremiah|Lamentations|Ezekiel|Daniel|Hosea|Joel|Amos"
                    + "|Obadiah|Jonah|Micah|Nahum|Habakkuk|Zephaniah|Haggai"
                    + "|Zechariah|Malachi|Matthew|Mark|Luke|John|Acts|Romans"
                    + "|Galatians|Ephesians|Philippians|Colossians|Titus"
                    + "|Philememon|Hebrews|James|Jude|Revelation)";

            jstPattern = Pattern.compile(
                    "Joseph" + space + "Smith" + space + "Translation," + space
                        + bibleBookName
                        + space
                        + chapter
                        + ":"
                        + verses,
                    Pattern.MULTILINE | Pattern.CASE_INSENSITIVE
            );
            referencePattern = Pattern.compile(
                    "\\/study\\/scriptures\\/"
                    + "(?:ot|nt|bofm|dc-testament|pgp|jst)\\/"
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
                    + "(?:(?:\\/([0-9]+))?[.]?(?:([0-9]+(?:[-,][0-9]+)*)"
                    + "|study_intro[0-9])?)?"
                    + "[?]lang=" + language + "(#p[A-Za-z0-9_-]+)?"
            );
        }
    }

    public static void main(String[] args) {
        int year = -1;
        int month = -1;
        String language = "";

        if (args.length >= 3) {
            try {
                year = Integer.parseInt(args[0]);
                month = Integer.parseInt(args[1]);
                language = args[2];
            } catch (NumberFormatException e) {
                System.err.println("'" + year + "' or '" + month + "' is not an integer");
            }
        }

        if (year < 1971 || month < 0 || (month != 4 && month != 10) || year > 2050
                || (!language.equals("eng") && !language.equals("spa"))) {
            System.err.println("Usage: SciCrawler year month language");
            System.err.println("    where year is 1971 to 2050, month is either 4 or 10,");
            System.err.println("    and language is either eng or spa");
            System.exit(-1);
        }

        SciCrawler crawler = new SciCrawler(year, month, language);

        crawler.getTableOfContents();
        crawler.crawlTalks();
    }

    private void addFilteredLink(List<Link> links, String talkId, String href, String text) {
        String[] tags = {"scriptures/bd", "scriptures/gs", "scriptures/tg"};

        for (String tag : tags) {
            if (href.contains(tag)) {
                return;
            }
        }

        Link link = new Link(talkId, href, text);

        link.addParsedToList(links);
    }

    private void crawlTalks() {
        List<Link> scriptureLinks = new ArrayList<>();

        File languageSubfolder = new File(conferencePath + "/" + language);

        if (!languageSubfolder.exists()) {
            languageSubfolder.mkdirs();
        }

        // For each talkId, get its contents JSON
        talkIds.forEach((talkId) -> {
            System.out.println("Get " + talkId);

            File talkFile = new File(conferencePath + "/" + talkId + HYPHEN
                    + language + ".json");
            JSONObject talkJson;

            if (!talkFile.exists()) {
                // Save talk if we haven't yet crawled it
                talkJson = new JSONObject(urlContent(talkUrl(talkId, language)));

                FileUtils.writeStringToFile(talkJson.toString(2), talkFile);
            } else {
                talkJson = new JSONObject(FileUtils.stringFromFile(talkFile));
            }

            JSONObject talkContent = talkJson.getJSONObject("content");
            String talkBody = talkContent.getString("body");
            extractScriptureLinks(talkBody, talkId, scriptureLinks);
            extractJstLinks(talkBody, talkId, scriptureLinks);

            if (talkContent.has("footnotes")) {
                JSONObject footnotes = talkContent.getJSONObject("footnotes");

                extractLinksFrom(footnotes, talkId, scriptureLinks);
            }

            writeHtmlContent(talkId, talkJson);
        });

        for (int i = 0; i < scriptureLinks.size(); i++) {
            Link link = scriptureLinks.get(i);

            System.out.println((i + 1) + "\t" + link.talkId + "\t" + link.href
                    + "\t" + link.text + "\t" + link.book + "\t" + link.chapter
                    + "\t" + link.verses + "\t" + link.isJst);
        }
    }

    private String encodeSpecialCharacters(String text) {
        return text
                .replace("“", "&#201C;")
                .replace("”", "&#201D;")
                .replace("’", "&#2019;")
                .replace("—", "&#2014;")
                .replace("…", "&#2026;")
                .replace(NDASH, "&#2013;");
//        return text
//                .replace("“", "&ldquo;")
//                .replace("”", "&rdquo;")
//                .replace("’", "&rsquo;")
//                .replace("—", "&mdash;")
//                .replace("…", "&hellip;")
//                .replace(NDASH, "&ndash;");
    }

    private void extractJstLinks(String content, String talkId, List<Link> links) {
        Matcher matcher = jstPattern.matcher(content);
        Book bookFinder = new Book();

        while (matcher.find()) {
            String fullMatch = matcher.group(0);
            String book = matcher.group(1);
            String chapter = matcher.group(2);
            String verses = matcher.group(3);
            String bookAbbr = bookFinder.abbreviationForBook(book);
            String volume = bookFinder.volumeForBook(book);

            links.add(new Link(
                    talkId, "/study/scriptures/" + volume + "/"
                        + bookAbbr + "/" + chapter + "." + verses + "?lang="
                        + language + "#p" + firstVerse(verses),
                    fullMatch, book, chapter, verses, true));
        }
    }

    private String firstVerse(String verses) {
        String[] parts = verses.split("[-,]");

        if (parts.length > 0) {
            return parts[0];
        }

        return verses;
    }

    private String extractMatch(String text, String regex) {
        Matcher matcher = Pattern.compile(regex, Pattern.MULTILINE | Pattern.DOTALL).matcher(text);

        if (matcher.find()) {
            return matcher.group(1);
        }

        return null;
    }

    private void extractLinksFrom(JSONObject footnotes, String talkId, List<Link> links) {
        String[] keys = new String[footnotes.keySet().size()];

        footnotes.keySet().toArray(keys);
        Arrays.sort(keys, new SortByNote());

        for (String key : keys) {
            JSONObject footnote = footnotes.getJSONObject(key);

            if (footnote.has("referenceUris")) {
                JSONArray uris = footnote.getJSONArray("referenceUris");

                uris.forEach((subitem) -> {
                    JSONObject uri = (JSONObject) subitem;

                    if (uri.has("href") && uri.has("type") && uri.has("text")
                            && uri.getString("type").equals("scripture-ref")) {
                        addFilteredLink(links, talkId, uri.getString("href"), uri.getString("text"));
                    }
                });
            }

            extractJstLinks(footnote.getString("text"), talkId, links);
        }
    }

    private void extractScriptureLinks(String content, String talkId, List<Link> links) {
        Matcher matcher = Pattern.compile("<a\\s+class=\"scripture-ref\"\\s+href=\"([^\"]+)\"[^>]*>([^<]*)<").matcher(content);

        while (matcher.find()) {
            addFilteredLink(links, talkId, matcher.group(1), matcher.group(2));
        }
    }

    private int filterTalkSubitem(String itemHref, String itemTitle, String itemSpeaker, int sessionNumber, int talkNumber) {
        String itemId = extractMatch(itemHref, "[0-9]+/[0-9]+/(.*)\\?");

        if (itemTitle.contains("Auditing Department")
                || itemTitle.contains("Sustaining of General Authorities")) {
            return talkNumber;
        }

        ++talkNumber;

        talkIds.add(itemId);
        talkHrefs.put(itemId, itemHref);
        talkTitles.put(itemId, itemTitle);
        talkSpeakers.put(itemId, itemSpeaker);

        System.out.println(itemId
                + "\t" + talkNumber
                + "\t" + sessionNumber
                + "\t" + encodeSpecialCharacters(itemTitle)
                + "\t" + itemSpeaker);

        return talkNumber;
    }

    private void formatFootnotes(StringBuilder talk, JSONObject footnotes) {
        /*
            "note4": {
                "marker": "4.",
                "context": null,
                "pid": "143139345",
                "id": "note4",
                "text": "<p data-aid=\"143139369\" id=\"note4_p1\">See
                    <a class=\"scripture-ref\" href=\"/study/scriptures/dc-testament/dc/8.2-3?lang=eng#p2\">Doctrine
                    and Covenants 8:2&#x2013;3<\/a>.<\/p>",
                "referenceUris": [{
                  "href": "/study/scriptures/dc-testament/dc/8.2-3?lang=eng#p2",
                  "text": "Doctrine and Covenants 8:2\u20133",
                  "type": "scripture-ref"
                }]
              },
        */

        talk.append("\n<footer class=\"notes\">\n");
        talk.append("<p class=\"title\" id=\"note_title1\">Notes</p>\n");
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

    private void getTableOfContents() {
        String content = tableOfContentsHtml();

        if (content != null) {
            String subitems = talkSubitemsFromTableOfContents(content);

            parseTalkSubitems(subitems);
        }
    }

    private String inlinedFootnote(String noteKey, JSONObject footnotes) {
        /*
            Original marker markup:
                <a class="note-ref" href="#note1">
                    <sup class="marker">1</sup>
                </a>

            Inlined footnote markup:
                <sup class="noteMarker">
                    <a href="#note1">1</a>
                    <span class="footnote">[...]</span>
                </sup>

            Footnote markup:
                <li data-marker="1." id="note1">
                    <p data-aid="141535577" id="note1_p1">...</p>
                </li>

            JSON format:
                "note1": {
                  "marker": "1.",
                  "context": null,
                  "pid": "141535487",
                  "id": "note1",
                  "text": "<p data-aid=\"141535577\" id=\"note1_p1\">...<\/p>",
                  "referenceUris": [
                    {
                      "href": "/study/scriptures/nt/matt/20.30-34?lang=eng#p30",
                      "text": "Matthew 20:30\u201334",
                      "type": "scripture-ref"
                    },
                    {
                      "href": "/study/scriptures/nt/mark/10.46-52?lang=eng#p46",
                      "text": "Mark 10:46\u201352",
                      "type": "scripture-ref"
                    }
                  ]
                }
        */

        JSONObject footnote = footnotes.getJSONObject("note" + noteKey);
        String footnoteText = footnote.getString("text")
                .replaceFirst("^<p[^>]*>", "")
                .replaceFirst("<\\/p>$", "");

        return "<sup class=\"noteMarker\">"
                + "<a href=\"#note" + noteKey + "\">" + noteKey + "</a>"
                + "<span class=\"footnote\">[" + footnoteText + "]</span>"
                + "</sup>";
    }

    private void inlineFootnotes(StringBuilder talk, JSONObject footnotes) {
        String markerParser = "<a\\s*class=\"note-ref\"\\s*href=\"#note([0-9]+)\">"
                + "\\s*<sup\\s*class=\"marker\">\\s*([0-9]+)\\s*</sup>"
                + "\\s*</a>";
        Matcher markerMatcher = Pattern.compile(markerParser).matcher(talk);

        while (markerMatcher.find()) {
            talk.replace(
                    markerMatcher.start(),
                    markerMatcher.end(),
                    inlinedFootnote(markerMatcher.group(1), footnotes)
            );
        }
    }

    private int parseSessionItems(String sessionItems, int sessionNumber, int talkNumber) {
        String itemParser = "<li>\\s*<a" + CLASS_PARSER + "item"
                + END_CLASS_PARSER
                + "href=\"([^\"]+)\""
                + END_TAG_PARSER
                + "\\s*<div" + CLASS_PARSER + "itemTitle" + END_CLASS_PARSER + END_TAG_PARSER
                + "(?:<span\\s+class=activeMarker[^>]*>\\s*<\\/span>)?"
                + "\\s*<p>\\s*<span>\\s*([^<]*)\\s*<\\/span>\\s*<\\/p>\\s*"
                + "<p" + CLASS_PARSER + "subtitle" + END_CLASS_PARSER + END_TAG_PARSER
                + "\\s*([^<]*)\\s*<\\/p>"
                + "\\s*<\\/div>\\s*<\\/a>\\s*<\\/li>";

        Matcher itemMatcher = Pattern.compile(itemParser).matcher(sessionItems);

        while (itemMatcher.find()) {
            talkNumber = filterTalkSubitem(
                    itemMatcher.group(1),
                    itemMatcher.group(2),
                    itemMatcher.group(3),
                    sessionNumber,
                    talkNumber
            );
        }

        return talkNumber;
    }

    private void parseTalkSubitems(String subitems) {
        int sessionNumber = 0;
        int talkNumber = 0;
        String subitemParser = "<li>\\s*<(?:a|span)" + CLASS_PARSER + "sectionTitle"
                + END_CLASS_PARSER + END_TAG_PARSER
                + "\\s*<div" + CLASS_PARSER + "itemTitle"
                + END_CLASS_PARSER + END_TAG_PARSER
                + "(?:<span\\s+class=activeMarker[^>]*>\\s*<\\/span>)?"
                + "\\s*<p>\\s*<span>\\s*(.*?)\\s*<\\/span>\\s*<\\/p>\\s*"
                + "\\s*<\\/div>\\s*<\\/(?:a|span)>\\s*"
                + "(?:"
                + "\\s*<ul" + CLASS_PARSER + "subItems" + END_CLASS_PARSER
                + END_TAG_PARSER
                + "\\s*(.*?)"
                + "\\s*<\\/ul>"
                + ")?\\s*<\\/li>";
        Matcher matcher = Pattern.compile(subitemParser, Pattern.DOTALL).matcher(subitems);

        while (matcher.find()) {
            String sessionTitle = matcher.group(1).trim();
            String sessionItems = matcher.group(2);

            if (sessionTitle.contains("Session")) {
                ++sessionNumber;
                talkNumber = parseSessionItems(sessionItems, sessionNumber, talkNumber);
            }
        }
    }

    private List<String> sortedKeys(JSONObject footnotes) {
        List<String> keys = new ArrayList<>(footnotes.keySet().size());

        for (String key : footnotes.keySet()) {
            keys.add(key);
        }

        keys.sort(new Comparator<String>() {
            @Override
            public int compare(String k1, String k2) {
                int i1 = Integer.parseInt(k1.replaceAll("[^0-9]*", ""));
                int i2 = Integer.parseInt(k2.replaceAll("[^0-9]*", ""));

                return i1 - i2;
            }
        });

        return keys;
    }

    private String tableOfContentsHtml() {
        File conferenceDirectoryFile = new File(conferencePath);
        File cachedContentFile = new File(conferencePath + "/contents.html");
        String content;

        if (!conferenceDirectoryFile.exists()) {
            conferenceDirectoryFile.mkdir();
        }

        if (cachedContentFile.exists()) {
            content = FileUtils.stringFromFile(cachedContentFile);
        } else {
            content = urlContent(urlForConferenceContents());
            FileUtils.writeStringToFile(content, cachedContentFile);
        }

        return content;
    }

    private String talkHtmlContentForJson(JSONObject talkJson) {
        StringBuilder talk = new StringBuilder();

        JSONObject talkContent = talkJson.getJSONObject("content");
        String talkBody = talkContent.getString("body");
        JSONObject footnotes = null;

        if (talkContent.has("footnotes")) {
            footnotes = talkContent.getJSONObject("footnotes");
        }

        /*
         * 1. Remove link and video tags
         * 2. Format footnote markers, incoming form:
         *        <a class="note-ref" href="#note1"><sup class="marker">1</sup></a>
         *    convert to:
         *        <sup class="noteMarker"><a href="#note1">1</a><span class="footnote">[...]</span></span>
         *    where:
         *        the "..." is the properly marked-up inlined footnote text
         * 3. Append list of footnotes (if any)
         * 4. Wrap all in <div class="body">...</div>
         */
        talkBody = talkBody
                .replaceAll("<link[^>]*>", "")
                .replaceAll("<video[^>]*>.*<\\/video>", "")
                .replaceAll("<img[^>]*>", "")
                ;

        talk.append("<div class=\"body\">");
        talk.append(talkBody);

        if (footnotes != null) {
            formatFootnotes(talk, footnotes);
//            inlineFootnotes(talk, footnotes);
        }

        talk.append("</div>");

        return encodeSpecialCharacters(
                talk.toString()
//                        .replaceAll("><", ">\n<")
//                        .replaceAll("> ", ">\n")
//                        .replaceAll("\n\n", "\n")
        );
    }
    /*
    NEEDSWORK: access database to know citation number?

    Source text example:
        <a class="scripture-ref" href="/study/scriptures/nt/matt/20.30-34?lang=eng#p30">Matthew 20:30&#x2013;34</a>

    Target replacement example:
        <span class="citation" id="135301">
        <a href="javascript:void(0)" onclick="sx(this, 135301)">&#xA0;</a>
        <a href="javascript:void(0)" onclick="gs(135301)">Matthew 20:30–34</a>
        </span>
    */

    private String talkSubitemsFromTableOfContents(String content) {
        // Extract the subitems <ul> from the document
        String contentParser = "<nav" + CLASS_PARSER + "tableOfContents"
                + END_CLASS_PARSER + END_TAG_PARSER
                + "\\s*<(?:a|span)" + CLASS_PARSER + "bookTitle" + END_CLASS_PARSER + END_TAG_PARSER
                + ".*?</(?:a|span)>"
                + "(.*?)<\\/nav>";

        return extractMatch(
                extractMatch(content, contentParser),
                "<ul" + END_TAG_PARSER + "(.*)</ul>"
        );
    }

    private String talkUrl(String talkId, String language) {
        return URL_BASE + "/study/api/v3/language-pages/type/content?lang="
                + language + "&uri=%2F"
                + (language.equals("eng") ? "ensign" : "liahona")
                + "%2F" + year + "%2F" + monthString + "%2F" + talkId;
    }

    private String urlContent(String url) {
        String content = null;
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);

        httpGet.addHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.11 Safari/537.36");
        httpGet.addHeader("Accept-Charset", "utf-8");

        try {
            CloseableHttpResponse httpResponse = httpClient.execute(httpGet);

            content = new String(httpResponse.getEntity().getContent().readAllBytes(),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace(System.err);
        }

        httpGet.releaseConnection();

        return content;
    }

    private String urlForConferenceContents() {
        return URL_ENSIGN + "/" + year + "/" + monthString + "?lang=eng";
    }

    private void writeHtmlContent(String talkId, JSONObject talkJson) {
        FileUtils.writeStringToFile(
                talkHtmlContentForJson(talkJson),
                new File(conferencePath + "/" + language + "/" + talkId)
        );
    }

    private class Book {
        private static final int newTestamentIndex = 39;
        private static final String space = "(?:\\s| |&#x[aA]0;|&#160;)+";
        private static final String aAccent = "(?:a|á|&#x[eE]1;|&#aacute;)";
        private static final String eAccent = "(?:e|é|&#x[eE]9;|&#eacute;)";
        private static final String EAccent = "(?:E|É|&#x[cC]9;|&#Eacute;)";
        private static final String iAccent = "(?:i|í|&#x[eE][dD];|&#iacute;)";
        private static final String oAccent = "(?:o|ó|&#x[fF]3;|&#oacute;)";
        private static final String uAccent = "(?:u|ú|&#x[fF][aA];|&#uacute;)";
        private final List<String> abbreviations = List.of(
            "gen", "ex", "lev", "num", "deut", "josh", "judg", "ruth",
            "1-sam", "2-sam", "1-kgs", "2-kgs", "1-chr", "2-chr", "ezra",
            "neh", "esth", "job", "ps", "prov", "eccl", "song", "isa",
            "jer", "lam", "ezek", "dan", "hosea", "joel", "amos", "obad",
            "jonah", "micah", "nahum", "hab", "zeph", "hag", "zech", "mal",
            "matt", "mark", "luke", "john", "acts", "rom", "1-cor", "2-cor",
            "gal", "eph", "philip", "col", "1-thes", "2-thes", "1-tim",
            "2-tim", "titus", "philem", "heb", "james", "1-pet", "2-pet",
            "1-jn", "2-jn", "3-jn", "jude", "rev"
        );
        private final List<String> bookPatterns = List.of(
            "(Genesis|G" + eAccent + "nesis)",
            "(Exodus|" + EAccent + "xodo)",
            "(Leviticus|Lev" + iAccent + "tico)",
            "(Numbers|N" + uAccent + "meros)",
            "(Deuteronomy|Deuteronomio)",
            "(Joshua|Josu" + "eAccent)",
            "(Judges|Jueces)",
            "(Ruth|Rut)",
            "1" + space + "Samuel",
            "2" + space + "Samuel",
            "1" + space + "(Kings|Reyes)",
            "2" + space + "(Kings|Reyes)",
            "1" + space + "(Chronicles|Cr" + oAccent + "nicas)",
            "2" + space + "(Chronicles|Cr" + oAccent + "nicas)",
            "(Ezra|Esdras)",
            "(Nehemiah|Nehem" + iAccent + "as)",
            "(Esther|Ester)",
            "Job",
            "(Psalms?|Salmos?)",
            "(Proverbs|Proverbios)",
            "(Ecclesiastes|Eclesiast" + eAccent + "s)",
            "(Song" + space + "of" + space + "Solomon|Cantares)",
            "(Isaiah|Isa" + iAccent + "as)",
            "(Jeremiah|Jerem" + iAccent + "as)",
            "(Lamentations|Lamentaciones)",
            "(Ezekiel|Ezequiel)",
            "Daniel",
            "(Hosea|Oseas)",
            "Joel",
            "(Amos|Am" + oAccent + "s)",
            "(Obadiah|Abd" + iAccent + "as)",
            "(Jonah|Jon" + aAccent + "s)",
            "(Micah|Miqueas)",
            "(Nahum|Nah" + uAccent + "m)",
            "(Habakkuk|Habacuc)",
            "(Zephaniah|Sofon" + iAccent + "as)",
            "(Haggai|Hageo)",
            "(Zechariah|Zacar" + iAccent + "as)",
            "(Malachi|Malaqu" + iAccent + "as)",
            "(Matthew|Mateo)",
            "(Mark|Marcos)",
            "(Luke|Lucas)",
            "(John|Juan)",
            "(Acts|Hechos)",
            "(Romans|Romanos)",
            "1" + space + "(Corinthians|Corintios)",
            "2" + space + "(Corinthians|Corintios)",
            "(Galatians|G" + aAccent + "latas)",
            "(Ephesians|Efesios)",
            "(Philippians|Filipenses)",
            "(Colossians|Colosenses)",
            "1" + space + "(Thessalonians|Tesalonicenses)",
            "2" + space + "(Thessalonians|Tesalonicenses)",
            "1" + space + "(Timothy|Timoteo)",
            "2" + space + "(Timothy|Timoteo)",
            "(Titus|Tito)",
            "(Philememon|Filem" + oAccent + "n)",
            "(Hebrews|Hebreos)",
            "(James|Santiago)",
            "1" + space + "(Peter|Pedro)",
            "2" + space + "(Peter|Pedro)",
            "1" + space + "(John|Juan)",
            "2" + space + "(John|Juan)",
            "3" + space + "(John|Juan)",
            "(Jude|Judas)",
            "(Revelation|Apocalipsis)"
        );

        public String abbreviationForBook(String book) {
            int index = indexOfBook(book);

            return abbreviations.get(index);
        }

        public int indexOfBook(String book) {
            for (int i = 0; i < bookPatterns.size(); i++) {
                Matcher matcher = Pattern.compile(bookPatterns.get(i)).matcher(book);

                if (matcher.matches()) {
                    return i;
                }
            }

            return -1;
        }

        public String volumeForBook(String book) {
            int index = indexOfBook(book);

            return (index < newTestamentIndex) ? "ot" : "nt";
        }
    }

    private class Link {

        String talkId;
        String href;
        String text;
        String book;
        String chapter;
        String verses;
        boolean isJst;

        Link(String talkId, String href, String text) {
            this.talkId = talkId;
            this.href = href;
            this.text = text;
        }

        Link(String talkId, String href, String text, String book,
                String chapter, String verses, boolean isJst) {
            this.talkId = talkId;
            this.href = href;
            this.text = text;
            this.book = book;
            this.chapter = chapter;
            this.verses = verses;
            this.isJst = isJst;
        }

        Link(Link source) {
            this.talkId = source.talkId;
            this.href = source.href;
            this.text = source.text;
            this.book = source.book;
            this.chapter = source.chapter;
            this.verses = source.verses;
            this.isJst = source.isJst;
        }

        public void addParsedToList(List<Link> links) {
            links.add(this);

            Matcher matcher = referencePattern.matcher(href);

            if (matcher.find()) {
                book = matcher.group(1);
                chapter = matcher.group(2);
                verses = matcher.group(3);
                isJst = book.startsWith("jst-");

                JSONObject bookObject = bookObjectForThis();

                if (bookObject == null) {
                    System.out.println(">>>>>>>>>>>>> Unable to find book " + book);
                } else {
                    if (chapter != null) {
                        int maxVerse = books.maxVerseForBookIdChapter(bookObject.getInt("id"), Integer.parseInt(chapter), isJst);

                        if (verses != null) {
                            String[] verseList = verses.split("(,|" + HYPHEN + "|" + NDASH + ")");

                            for (String verse : verseList) {
                                int verseValue = Integer.parseInt(verse);

                                if (book.equals("js-h") && verseValue >= 76 && verseValue <= 82) {
                                    // NEEDSWORK: this is a reference to JS—H 1:endnote
                                    // which we should map to verse 1000 for our database
                                } else if (verseValue > maxVerse) {
                                    System.out.println(">>>>>>>>>>>>> Verse out of range "
                                            + book + " " + chapter + ":" + verses
                                            + " (" + verse + ")");
                                    break;
                                }
                            }
                        } else {
                            verses = maxVerse > 1 ? ("1-" + maxVerse) : "1";

                            addLinksForReferencedChaptersOtherThanChapter(links, integerValue(chapter));
                        }
                    } else {
                        // There is no chapter given; if the book should have
                        // a chapter, this could be a problem.
                        if (bookObject.getInt("numChapters") > 0) {
                            System.out.println(">>>>>>>>>>>>> Book should have chapter " + book);
                        }
                    }
                }
            } else {
                System.out.println(">>>>>>>>>>>>> No match for " + href);
            }
        }

        private void addLinkForReferencedChapter(List<Link> links, int chapter) {
            Link link = new Link(this);

            link.href = link.href.replace("/" + link.chapter, "/" + chapter);

            JSONObject bookObject = bookObjectForThis();
            int maxVerse = books.maxVerseForBookIdChapter(bookObject.getInt("id"), chapter, isJst);

            link.chapter = "" + chapter;
            link.verses = maxVerse > 1 ? ("1-" + maxVerse) : "1";
            links.add(link);
        }

        private void addLinksForReferencedChaptersOtherThanChapter(List<Link> links, int baseChapter) {
            int chapterIndex = text.indexOf(baseChapter + "");

            if (chapterIndex < 0) {
                chapterIndex = 0;
            }

            String[] parts = text.substring(chapterIndex)
                    .split(String.format(WITH_DELIMITER, "[^0-9]"));

            for (int i = 0; i < parts.length; i++) {
                int chapter = integerValue(parts[i]);

                if (chapter > 0 && i + 2 < parts.length) {
                    // Process a disjunction or a range
                    int endChapter = trailingChapterValue(parts[i], parts[i + 2]);

                    if ((parts[i + 1].contains(HYPHEN) || parts[i + 1].contains(NDASH))
                            && endChapter > 0) {
                        // This is a range
                        for (int chap = chapter; chap <= endChapter; chap++) {
                            if (chap != baseChapter) {
                                addLinkForReferencedChapter(links, chap);
                            }
                        }
                    } else {
                        // This is a disjunction
                        if (chapter > baseChapter && chapter > 0) {
                            addLinkForReferencedChapter(links, chapter);
                        }

                        if (endChapter > baseChapter && endChapter > 0) {
                            addLinkForReferencedChapter(links, endChapter);
                        }
                    }

                    i += 2;
                } else if (chapter > 0 && chapter > baseChapter) {
                    addLinkForReferencedChapter(links, chapter);
                }
            }
        }

        private JSONObject bookObjectForThis() {
            String bookKey = book;

            if (book.startsWith("jst-")) {
                bookKey = book.substring(4);
            }

            return books.bookForAbbreviation(bookKey);
        }

        private int integerValue(String text) {
            try {
                return Integer.parseInt(text);
            } catch (NumberFormatException e) {
                return 0;
            }
        }

        private int trailingChapterValue(String chapter1, String chapter2) {
            String finalChapter = chapter2;

            if (chapter2.length() < chapter1.length()) {
                finalChapter = chapter1.substring(0, chapter1.length() - chapter2.length())
                        + chapter2;
            }

            return integerValue(finalChapter);
        }
    }

    private class SortByNote implements Comparator<String> {

        @Override
        public int compare(String arg0, String arg1) {
            if (arg0 != null && arg1 != null && arg0.length() > 4 && arg1.length() > 4) {
                try {
                    int n0 = Integer.parseInt(arg0.substring(4));
                    int n1 = Integer.parseInt(arg1.substring(4));

                    return n0 - n1;
                } catch (NumberFormatException e) {
                    e.printStackTrace(System.err);
                }
            }

            return (arg0 != null && arg1 != null) ? arg0.compareTo(arg1) : 0;
        }
    }
}
