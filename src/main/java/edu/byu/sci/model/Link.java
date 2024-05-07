package edu.byu.sci.model;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONObject;

import edu.byu.sci.crawler.SciCrawler;
import edu.byu.sci.util.Utils;

public class Link {
    private static final String WITH_DELIMITER = "((?<=%1$s)|(?=%1$s))";

    private static final Logger logger = Logger.getLogger(Link.class.getName());
    private static final Books books = new Books();

    public int citationId;
    public String talkId;
    public String href;
    public String text;
    public String book;
    public String chapter;
    public String verses;
    public int page;
    public boolean isJst;
    public boolean isDeleted = false;
    public String footnoteKey = "";

    public Link(String talkId, String href, String text, String footnoteKey) {
        this.talkId = talkId;
        this.href = href;
        this.text = text;
        this.footnoteKey = footnoteKey;
    }

    public Link(String href, String page, String talkId, String text, String language, Pattern referencePattern) {
        this.page = Utils.integerValue(page);
        this.talkId = talkId;
        this.text = text;

        Matcher matcher = Pattern.compile("^(?:jst-)?([^/]+)[/]([0-9-]+)[.]?([0-9-]*)$").matcher(href);

        if (matcher.find()) {
            String bookGroup = matcher.group(1);

            chapter = matcher.group(1);
            verses = matcher.group(2);

            this.href = SciCrawler.URL_SCRIPTURE_PATH + BookFinder.sInstance.volumeForBook(bookGroup)
                    + SciCrawler.PATH_SEPARATOR
                    + href.replace("bofm-intro", "introduction").replace("dc-intro", "introduction") + "?lang="
                    + language + "#p" + Link.firstVerse(verses);
        } else {
            logger.log(Level.SEVERE, () -> "Ill-formed href: " + href);
            System.exit(-1);
            this.href = href;
        }

        List<Link> parsedLinks = new ArrayList<>();

        addParsedToList(parsedLinks, referencePattern);
    }

    public Link(String talkId, String href, String text, String book, String chapter, String verses, boolean isJst,
            String footnoteKey) {
        this.talkId = talkId;
        this.href = href;
        this.text = text;
        this.book = book;
        this.chapter = chapter;
        this.verses = verses;
        this.isJst = isJst;
        this.footnoteKey = footnoteKey;
    }

    public Link(Link source) {
        this.talkId = source.talkId;
        this.href = source.href;
        this.text = source.text;
        this.book = source.book;
        this.chapter = source.chapter;
        this.verses = source.verses;
        this.isJst = source.isJst;
        this.page = source.page;
        this.footnoteKey = source.footnoteKey;
    }

    public void addParsedToList(List<Link> links, Pattern referencePattern) {
        links.add(this);

        Matcher matcher = referencePattern.matcher(href);

        if (matcher.find()) {
            book = matcher.group(1);
            chapter = matcher.group(2);
            verses = matcher.group(3);
            isJst = book.startsWith("jst-");

            String altVerses = matcher.group(4);

            if (verses == null && altVerses != null && !altVerses.isEmpty()) {
                verses = altVerses.replace("%2C", ",").replace("p", "");
            }

            JSONObject bookObject = bookObjectForThis();

            if (bookObject == null) {
                logger.log(Level.WARNING, () -> ">>>>>>>>>>>>> Unable to find book " + book);
            } else {
                if (chapter != null) {
                    int maxVerse = books.maxVerseForBookIdChapter(bookObject.getInt("id"), Integer.parseInt(chapter),
                            isJst);

                    if (verses != null) {
                        String[] verseList = verses.split("([," + SciCrawler.HYPHEN + SciCrawler.NDASH + "])");

                        correctVersesIfNeeded(verses, chapter);

                        for (String verse : verseList) {
                            int verseValue = Integer.parseInt(verse);

                            if (book.equals("js-h") && verseValue >= 76 && verseValue <= 82) {
                                // NEEDSWORK: this is a reference to JS—H 1:endnote
                                // which we should map to verse 1000 for our database
                            } else if (verseValue > maxVerse) {
                                logger.log(Level.WARNING, () -> ">>>>>>>>>>>>> Verse out of range " + book + " "
                                        + chapter + ":" + verses + " (" + verse + ")");
                                break;
                            }
                        }
                    } else {
                        addLinksForReferencedChaptersOtherThanChapter(links, Utils.integerValue(chapter));
                        this.verses = maxVerse > 1 ? ("1-" + maxVerse) : "1";

                        if (!href.contains(".")) {
                            href = href.replace("?", "." + this.verses + "?");
                        }
                    }
                } else {
                    // There is no chapter given; if the book should have
                    // a chapter, this could be a problem.
                    if (bookObject.getInt("numChapters") > 0) {
                        logger.log(Level.WARNING, () -> ">>>>>>>>>>>>> Book should have chapter " + book);
                    }
                }
            }
        } else {
            logger.log(Level.WARNING, () -> ">>>>>>>>>>>>> No match for " + href);
            System.exit(-2);
        }
    }

    private void addLinkForReferencedChapter(List<Link> links, int chapter) {
        Link link = new Link(this);

        link.href = link.href.replace("/" + link.chapter, "/" + chapter);

        JSONObject bookObject = bookObjectForThis();
        int maxVerse = books.maxVerseForBookIdChapter(bookObject.getInt("id"), chapter, isJst);

        link.chapter = "" + chapter;
        link.verses = maxVerse > 1 ? ("1-" + maxVerse) : "1";

        if (!link.href.contains(".")) {
            if (link.href.contains("?")) {
                link.href = link.href.replace("?", "." + link.verses + "?");
            } else {
                link.href += "." + link.verses;
            }
        }

        links.add(link);
    }

    // NEEDSWORK: it would be sweet to automate this processing so we rewrite the
    // talk to include
    // the additional hyperlinks implied by the disjunctions and/or ranges we find
    private void addLinksForReferencedChaptersOtherThanChapter(List<Link> links, int baseChapter) {
        int chapterIndex = text.indexOf(baseChapter + "");

        if (chapterIndex < 0) {
            chapterIndex = 0;
        }

        String[] parts = text.substring(chapterIndex).split(String.format(WITH_DELIMITER, "[^0-9]"));

        for (int i = 0; i < parts.length; i++) {
            int chapterValue = Utils.integerValue(parts[i]);

            if (chapterValue > 0 && i + 2 < parts.length) {
                // Process a disjunction or a range
                int endChapter = trailingChapterValue(parts[i], parts[i + 2]);

                if ((parts[i + 1].contains(SciCrawler.HYPHEN) || parts[i + 1].contains(
                        SciCrawler.NDASH)) && endChapter > 0) {
                    // This is a range
                    for (int chap = chapterValue; chap <= endChapter; chap++) {
                        if (chap != baseChapter) {
                            addLinkForReferencedChapter(links, chap);
                        }
                    }
                } else {
                    // This is a disjunction
                    if (chapterValue > baseChapter && chapterValue > 0) {
                        addLinkForReferencedChapter(links, chapterValue);
                    }

                    if (endChapter > baseChapter && endChapter > 0) {
                        addLinkForReferencedChapter(links, endChapter);
                    }
                }

                i += 2;
            } else if (chapterValue > 0 && chapterValue > baseChapter) {
                addLinkForReferencedChapter(links, chapterValue);
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

    private void correctVersesIfNeeded(String verses, String chapter) {
        if (!verses.contains(",")) {
            return;
        }

        StringBuilder canonicalVerses = new StringBuilder();
        String[] components = verses.split(",");
        int previousValue = -1;
        boolean needsComma = false;

        for (String component : components) {
            if (component.contains(SciCrawler.HYPHEN) || component.contains(SciCrawler.NDASH)) {
                if (needsComma) {
                    canonicalVerses.append(",");
                }

                previousValue = -1;
                canonicalVerses.append(component);
            } else {
                int verseValue = Utils.integerValue(component);

                if (verseValue == previousValue + 1) {
                    canonicalVerses.append(SciCrawler.HYPHEN);
                    previousValue = -1;
                } else {
                    if (needsComma) {
                        canonicalVerses.append(",");
                    }

                    previousValue = verseValue;
                }

                canonicalVerses.append(component);
            }

            needsComma = true;
        }

        if (!verses.equals(canonicalVerses.toString())) {
            this.verses = canonicalVerses.toString();
            this.href = this.href.replace(chapter + "." + verses, chapter + "." + this.verses);
        }
    }

    private String textTransform(String str) {
        return str
                .replace("\u00A0", " ")
                .replace("\u2013", "-")
                .replace("–", "-")
                .replace("—", "-");
    }

    private boolean textIsSimilar(String text1, String text2) {
        if (text1 == null || text2 == null) {
            return text1 == null && text2 == null;
        }

        return textTransform(text1).equals(textTransform(text2));
    }

    public boolean isEqualTo(Link link) {
        if (verses == null || chapter == null) {
            // Compare on the non-chapter/verse elements
            return isDeleted == link.isDeleted
                    && isJst == link.isJst
                    && page == link.page
                    && book.equals(link.book)
                    && textIsSimilar(text, link.text)
                    && href.equals(link.href)
                    && talkId.equals(link.talkId)
                    && (footnoteKey.equals(link.footnoteKey) || footnoteKey.equals("body")
                            || link.footnoteKey.equals("body"));
        }

        return isDeleted == link.isDeleted
                && isJst == link.isJst
                && page == link.page
                && textIsSimilar(verses, link.verses)
                && chapter.equals(link.chapter)
                && book.equals(link.book)
                && textIsSimilar(text, link.text)
                && textIsSimilar(href, link.href)
                && talkId.equals(link.talkId)
                && (footnoteKey.equals(link.footnoteKey) || footnoteKey.equals("body")
                        || link.footnoteKey.equals("body"));
    }

    public static String firstVerse(String verses) {
        String[] parts = verses.split("[-–—,]");

        if (parts.length > 0) {
            return parts[0];
        }

        return verses;
    }

    private int trailingChapterValue(String chapter1, String chapter2) {
        String finalChapter = chapter2;

        if (chapter2.length() < chapter1.length()) {
            finalChapter = chapter1.substring(0, chapter1.length() - chapter2.length()) + chapter2;
        }

        return Utils.integerValue(finalChapter);
    }
}
