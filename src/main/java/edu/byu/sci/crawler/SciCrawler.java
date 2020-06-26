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

    private static final String HYPHEN = "-";
    private static final String NDASH = "–";

    private static final String URL_BASE = "https://www.churchofjesuschrist.org";
    private static final String URL_ENSIGN = URL_BASE + "/study/ensign";
    private static final String URL_LIAHONA = URL_BASE + "/study/liahona";

    private static final String WITH_DELIMITER = "((?<=%1$s)|(?=%1$s))";

    private final String monthString;
    private final int year;
    private final String conferencePath;

    private Pattern referencePattern;

    private final Books books = new Books();

    private final List<String> talkIds = new ArrayList<>();
    private final Map<String, String> talkHrefs = new HashMap<>();
    private final Map<String, String> talkSpeakers = new HashMap<>();
    private final Map<String, String> talkTitles = new HashMap<>();

    public SciCrawler(int year, int month) {
        this.year = year;
        monthString = month == 4 ? "05" : "11";
        conferencePath = year + (month == 4 ? "apr" : "oct");
    }

    public static void main(String[] args) {
        int year = -1;
        int month = -1;

        if (args.length >= 2) {
            try {
                year = Integer.parseInt(args[0]);
                month = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.err.println("'" + year + "' or '" + month + "' is not an integer");
            }
        }

        if (year < 1971 || month < 0 || (month != 4 && month != 10) || year > 2050) {
            System.err.println("Usage: SciCrawler year month");
            System.err.println("    where year is 1970 to 2050 and month is either 4 or 10");
            System.exit(-1);
        }

        SciCrawler crawler = new SciCrawler(year, month);

        crawler.getTableOfContents();
        crawler.crawl("eng");
//        crawler.crawl("spa");
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

    private void crawl(String language) {
        List<Link> scriptureLinks = new ArrayList<>();

        String pattern = "\\/study\\/scriptures\\/"
                + "(?:ot|nt|bofm|dc-testament|pgp|jst)\\/"
                + "((?:jst-)?(?:gen|ex|lev|num|deut|josh|judg|ruth|1-sam|2-sam|1-kgs|2-kgs|1-chr|2-chr|ezra|neh|esth|job|ps|prov|eccl|song|isa|jer|lam|ezek|dan|hosea|joel|amos|obad|jonah|micah|nahum|hab|zeph|hag|zech|mal|matt|mark|luke|john|acts|rom|1-cor|2-cor|gal|eph|philip|col|1-thes|2-thes|1-tim|2-tim|titus|philem|heb|james|1-pet|2-pet|1-jn|2-jn|3-jn|jude|rev|bofm-title|introduction|three|eight|1-ne|2-ne|jacob|enos|jarom|omni|w-of-m|mosiah|alma|hel|3-ne|4-ne|morm|ether|moro|dc|od|moses|abr|js-m|js-h|fac-1|fac-2|fac-3|a-of-f))"
                + "(?:(?:\\/([0-9]+))?[.]?(?:([0-9]+(?:[-,][0-9]+)*)|study_intro[0-9])?)?"
                + "[?]lang=" + language + "(#p[A-Za-z0-9_-]+)?";

        referencePattern = Pattern.compile(pattern);

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

            if (talkContent.has("footnotes")) {
                JSONObject footnotes = talkContent.getJSONObject("footnotes");

                extractLinksFrom(footnotes, talkId, scriptureLinks);
            }
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
                .replace("“", "&ldquo;")
                .replace("”", "&rdquo;")
                .replace("’", "&rsquo;")
                .replace("—", "&mdash;")
                .replace(NDASH, "&ndash;");
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
        }
    }

    private void extractScriptureLinks(String content, String talkId, List<Link> links) {
        Matcher matcher = Pattern.compile("<a\\s+class=\"scripture-ref\"\\s+href=\"([^\"]+)\"[^>]*>([^<]*)<").matcher(content);

        while (matcher.find()) {
            addFilteredLink(links, talkId, matcher.group(1), matcher.group(2));
        }
    }

    private void getTableOfContents() {
        int sessionNumber = 0;
        int talkNumber = 0;
        String url = URL_ENSIGN + "/" + year + "/" + monthString + "?lang=eng";
        File conferenceDirectory = new File(conferencePath);
        File cachedContent = new File(conferencePath + "/contents.html");
        String content;

        if (!conferenceDirectory.exists()) {
            conferenceDirectory.mkdir();
        }

        if (cachedContent.exists()) {
            content = FileUtils.stringFromFile(cachedContent);
        } else {
            content = urlContent(url);
            FileUtils.writeStringToFile(content, cachedContent);
        }

        if (content != null) {
            // Parse content looking for talks
            String classParser = "\\s+class=\"?";
            String endClass = "[^\">]*\"?\\s*";
            String endTag = "[^>]*>";

            // Extract the subitems <ul> from the document
            String contentParser = "<nav" + classParser + "tableOfContents"
                    + endClass + endTag
                    + "\\s*<a" + classParser + "bookTitle" + endClass + endTag
                    + ".*?</a>"
                    + "(.*?)<\\/nav>";

            String subitems = extractMatch(content, contentParser);

            subitems = extractMatch(subitems, "<ul" + endTag + "(.*)</ul>");

            String subitemParser = "<li>\\s*<a" + classParser + "sectionTitle" + endClass + endTag
                    + "\\s*<div" + classParser + "itemTitle" + endClass + endTag
                    + "(?:<span\\s+class=activeMarker[^>]*>\\s*<\\/span>)?"
                    + "\\s*<p>\\s*<span>\\s*(.*?)\\s*<\\/span>\\s*<\\/p>\\s*"
                    + "\\s*<\\/div>\\s*<\\/a>\\s*"
                    + "(?:"
                    + "\\s*<ul" + classParser + "subItems" + endClass + endTag
                    + "\\s*(.*?)"
                    + "\\s*<\\/ul>"
                    + ")?\\s*<\\/li>";
            Matcher matcher = Pattern.compile(subitemParser, Pattern.DOTALL).matcher(subitems);

            while (matcher.find()) {
                String sessionTitle = matcher.group(1).trim();
                String sessionItems = matcher.group(2);

                if (sessionTitle.contains("Session")) {
                    ++sessionNumber;

                    String itemParser = "<li>\\s*<a" + classParser + "item"
                            + endClass
                            + "href=\"([^\"]+)\""
                            + endTag
                            + "\\s*<div" + classParser + "itemTitle" + endClass + endTag
                            + "(?:<span\\s+class=activeMarker[^>]*>\\s*<\\/span>)?"
                            + "\\s*<p>\\s*<span>\\s*([^<]*)\\s*<\\/span>\\s*<\\/p>\\s*"
                            + "<p" + classParser + "subtitle" + endClass + endTag
                            + "\\s*([^<]*)\\s*<\\/p>"
                            + "\\s*<\\/div>\\s*<\\/a>\\s*<\\/li>";

                    Matcher itemMatcher = Pattern.compile(itemParser).matcher(sessionItems);

                    while (itemMatcher.find()) {
                        String itemHref = itemMatcher.group(1);
                        String itemTitle = itemMatcher.group(2);
                        String itemSpeaker = itemMatcher.group(3);
                        String itemId = extractMatch(itemHref, "[0-9]+/[0-9]+/(.*)\\?");

                        if (itemTitle.contains("Auditing Department")
                                || itemTitle.contains("Sustaining of General Authorities")) {
                            continue;
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
                    }
                }
            }
        }
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
            JSONObject bookObject = bookObjectForThis();
            int maxVerse = books.maxVerseForBookIdChapter(bookObject.getInt("id"), chapter, isJst);

            link.chapter = "" + chapter;
            link.verses = maxVerse > 1 ? ("1-" + maxVerse) : "1";
            links.add(link);
        }

        private void addLinksForReferencedChaptersOtherThanChapter(List<Link> links, int baseChapter) {
            String[] parts = text.split(String.format(WITH_DELIMITER, "[^0-9]+"));

            for (int i = 0; i < parts.length; i++) {
                int chapter = integerValue(parts[i]);

                if (chapter > 0 && i + 2 < parts.length) {
                    // Process a disjunction or a range
                    int endChapter = integerValue(parts[i + 2]);

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
