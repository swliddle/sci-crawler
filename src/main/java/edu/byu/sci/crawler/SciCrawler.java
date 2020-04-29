package edu.byu.sci.crawler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONObject;

public class SciCrawler {

    private static final String URL_BASE = "https://www.churchofjesuschrist.org";
    private static final String URL_ENSIGN = URL_BASE + "/study/ensign";

    private final int month;
    private final String monthString;
    private final int year;
    private final String conferencePath;

    private final List<String> talkIds = new ArrayList<>();
    private final Map<String, String> talkHrefs = new HashMap<>();
    private final Map<String, String> talkSpeakers = new HashMap<>();
    private final Map<String, String> talkTitles = new HashMap<>();

    public SciCrawler(int year, int month) {
        this.year = year;
        this.month = month;
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

        if (year < 2020 || month < 0 || (month != 4 && month != 10) || year > 2040) {
            System.err.println("Usage: SciCrawler year month");
            System.err.println("    where year is 2020 to 2040 and month is either 4 or 10");
            System.exit(-1);
        }

        SciCrawler crawler = new SciCrawler(year, month);

        crawler.getTableOfContents();
        crawler.crawl("eng");
//        crawler.crawl("spa");
    }

    private void crawl(String language) {
        List<Link> scriptureLinks = new ArrayList<>();

        // For each talkId, get its contents JSON
        talkIds.forEach((talkId) -> {
            System.out.println("Get " + talkId);

            File talkFile = new File(conferencePath + "/" + talkId + "-"
                    + language + ".json");
            JSONObject talkJson;

            if (!talkFile.exists()) {
                // Save talk if we haven't yet crawled it
                talkJson = new JSONObject(urlContent(talkUrl(talkId, language)));

                writeStringToFile(talkJson.toString(2), talkFile);
            } else {
                talkJson = new JSONObject(stringFromFile(talkFile));
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
                    + "\t" + link.text);
        }
    }

    private String encodeSpecialCharacters(String text) {
        return text
                .replace("“", "&ldquo;")
                .replace("”", "&rdquo;")
                .replace("’", "&rsquo;")
                .replace("—", "&mdash;")
                .replace("–", "&ndash;");
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
                        // NEEDSWORK: skip links that are /bd/, /gs/, etc.
                        links.add(new Link(talkId, uri.getString("href"), uri.getString("text")));
                    }
                });
            }
        }
    }

    private void extractScriptureLinks(String content, String talkId, List<Link> links) {
        Matcher matcher = Pattern.compile("<a\\s+class=\"scripture-ref\"\\s+href=\"([^\"]+)\"[^>]*>([^<]*)<").matcher(content);

        while (matcher.find()) {
            links.add(new Link(talkId, matcher.group(1), matcher.group(2)));
        }
    }

    private void getTableOfContents() {
        int sessionNumber = 0;
        int talkNumber = 0;
        String url = URL_ENSIGN + "/" + year + "/" + monthString + "?lang=eng";
        File cachedContent = new File(conferencePath + "/contents.html");
        String content;

        if (cachedContent.exists()) {
            content = stringFromFile(cachedContent);
        } else {
            content = urlContent(url);
            writeStringToFile(content, cachedContent);
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

    private String stringFromFile(File file) {
        StringBuilder contentBuilder = new StringBuilder();

        try ( Stream<String> stream = Files.lines(Paths.get(file.getPath()),
                StandardCharsets.UTF_8)) {
            stream.forEach(s -> contentBuilder.append(s).append("\n"));
        } catch (IOException e) {
            e.printStackTrace(System.err);
        }

        return contentBuilder.toString();
    }

    private String talkUrl(String talkId, String language) {
        return URL_BASE + "/study/api/v3/language-pages/type/content?lang="
                + language + "&uri=%2Fensign%2F"
                + year + "%2F" + monthString + "%2F" + talkId;
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

    private void writeStringToFile(String content, File file) {
        try {
            try ( FileOutputStream stream = new FileOutputStream(file)) {
                stream.write(content.getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            e.printStackTrace(System.err);
        }
    }

    private class Link {

        String talkId;
        String href;
        String text;

        Link(String talkId, String href, String text) {
            this.talkId = talkId;
            this.href = href;
            this.text = text;
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
