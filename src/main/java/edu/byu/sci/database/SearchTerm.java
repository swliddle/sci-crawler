package edu.byu.sci.database;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SearchTerm {
    private static final String ITALICS_SPAN = "<span class=\"italic\">";

    private final int colorIndex;
    private final int proximityCount;
    private String regularExpression;
    private final String[] termList;

    public SearchTerm(String term, int proximityCount, int colorIndex) {
        termList = term.split("\\s");
        this.proximityCount = proximityCount;
        this.colorIndex = colorIndex;
    }

    public static StringBuilder cleanHtmlString(String html) {
        StringBuilder mutableHtml = new StringBuilder(html.toLowerCase());

        replaceAll("<html.*</head>", mutableHtml);
        replaceAll("<div id=.ldsurl.*>[^<]*</div>", mutableHtml);
        replaceAll("<div id=.talkcrumb.*>[^<]*</div>", mutableHtml);

        replaceAll("<div class=.next.*?</div>", mutableHtml);
        replaceAll("<div class=.prev.*?</div>", mutableHtml);
        replaceAll("<div class=.pageNum.*?</div>", mutableHtml);
        replaceAll("<span class=.footRef.>[^<]*</span>", mutableHtml);
        replaceAll("<div class=.footnotes.>(.*?<div\\s+class=.footnote.>.*?</div>)*.*?</div>", mutableHtml);
        replaceAll("<span class=.textMark.>[^<]*</span>", mutableHtml);

        rejoinHyphenatedWordsInString(mutableHtml);

        replaceAll("<div class=.hyphen.>[\\p{Pd}]</div><div class=.break.*?</div>", mutableHtml);
        replaceAll("<div class=.break.*?</div>", mutableHtml);

        replaceAll("<span class=.ccontainer.><span class=.citation.*?</span></span>", mutableHtml);

        replaceAll("<script[^/>]+/>", mutableHtml);
        replaceAll("<script[^>]+>.*?</script>", mutableHtml);

        replaceAll("<div[ ]+id=.(skipnav|navheader|contentheader|footer|navigation)[^>]+>.*?</div>", mutableHtml);
        replaceAll("<div[ ]+class=.chapter-pagination[^>]+>.*?</div>", mutableHtml);
        replaceAll("<a[ ]+class=.noPrint[^>]+>.*?</a>", mutableHtml);
        replaceAll("<ul[ ]+class=.mediaformatbar2[^>]+>.*?</ul>", mutableHtml);
        replaceAll("<span[ ]+style=.display:[ ]+none[^>]+>[^<]*</span>", mutableHtml);

        replaceAll("<[^>]+>", mutableHtml);

        replaceAll("Å", "A", mutableHtml);
        replaceAll("ç", "c", mutableHtml);
        replaceAll("ñ", "n", mutableHtml);

        replaceAll("[àáãäâā]", "a", mutableHtml);
        replaceAll("[èéë]", "e", mutableHtml);
        replaceAll("[íï]", "i", mutableHtml);
        replaceAll("[óôõöø]", "o", mutableHtml);
        replaceAll("[úü]", "u", mutableHtml);
        replaceAll("[\u2018\u2019\u201a\u201b]", "'", mutableHtml);
        replaceAll("[°\u201c\u201d\u201e\u201f]", " ", mutableHtml);

        reducePossessiveAcronymsInString(mutableHtml);

        // II.ii.1 5151
        // www.familysearch.org 6518
        // www.mormon.org 6576
        // www.coachwooden.com, bio.shtml, success.shtml 6582
        // www.nodrugs.com, www.kidsource.com 6665
        // solarsystem.nasa.gov, sun.html 6806
        // 6811, 6814, 6879, 6890, 7041, 7043, 7048, 7051, 7113, 7131, 7179, 7182, 7201,
        // 7339,
        // 7355, 7408, 7420, 8421, 7484, 7497, 7500, 7506, 7514, 7517, 7567, 7606,

        // NEEDSWORK: we have the ligatures (ae and 1/2) to deal with still, but since
        // they expand the
        // string, the current approach doesn't quite work. Likewise, we still don't
        // highlight parts
        // of a word after hyphen divs.

        replaceAll("\u00a0", mutableHtml);
        replaceAll("[-\\p{Pd}]", mutableHtml);
        replaceAll(":", mutableHtml);
        replaceAll("[.][.]+", mutableHtml);

        replaceAll("&([a-z]{1,7}|#[0-9]{1,5});", mutableHtml);

        /*
         * [mutableHtml replaceOccurrencesOfString:@";" withString:@" " options:0
         * range:NSMakeRange(0, [mutableHtml length])); [mutableHtml
         * replaceOccurrencesOfString:@"-" withString:@" " options:0
         * range:NSMakeRange(0, [mutableHtml length]));
         */

        removePunctuation(mutableHtml);

        // Allowed characters: [a-z0-9_] embedded: [,/.']
        replaceAll("[^a-z0-9_\\s]+(\\s|$)", mutableHtml);
        replaceAll("(^|\\s)[^a-z0-9_\\s]+", mutableHtml);

        replaceAll("\\['", mutableHtml);
        replaceAll("[\"!?()=\\[\\]]", mutableHtml);
        replaceAll("[,.](\\s|$)", mutableHtml);

        return mutableHtml;
    }

    public int getColorIndex() {
        return colorIndex;
    }

    public String getDescription() {
        StringBuilder desc = new StringBuilder();

        for (String term : termList) {
            if (desc.length() > 0) {
                desc.append(" ");
            }

            desc.append(term);
        }

        if (termList.length > 1) {
            desc.insert(0, "\"").append("\"");
        }

        if (proximityCount > 0) {
            desc.append("~").append(proximityCount);
        }

        return desc.toString();
    }

    public int getProximityCount() {
        return proximityCount;
    }

    public String getRegularExpression() {
        if (regularExpression == null) {
            StringBuilder regexpBuilder = new StringBuilder();

            if (proximityCount > 0 && termList.length > 1) {
                StringBuilder termExpression = new StringBuilder();

                // If we have a two-word term list, check for A...B or B...A
                if (termList.length <= 2) {
                    String term1 = generalizedExpressionForTerm(termList[0]);
                    String term2 = generalizedExpressionForTerm(termList[1]);

                    regexpBuilder.append("(").append(term1).append("\\s+(\\S+\\s+){0,").append(proximityCount + 1)
                            .append("}").append(term2).append("|").append(term2).append("\\s+(\\S+\\s+){0,")
                            .append(proximityCount + 1).append("}").append(term1).append(")");
                } else {
                    // With an N-term list, allow any combination of A1...A2...An
                    // in any order, including repeats of the Ai.
                    for (String term : termList) {
                        if (termExpression.length() > 0) {
                            termExpression.append("|");
                        }

                        termExpression.append(generalizedExpressionForTerm(term));
                    }

                    termExpression.insert(0, "(").append(")");

                    for (int i = termList.length; i > 0; --i) {
                        if (regexpBuilder.length() > 0) {
                            regexpBuilder.append("\\s*(\\S+\\s+){0,").append(proximityCount + 1).append("}");
                        }

                        regexpBuilder.append(termExpression);
                    }
                }
            } else {
                for (String term : termList) {
                    if (regexpBuilder.length() > 0) {
                        regexpBuilder.append("\\s+");
                    }

                    regexpBuilder.append(generalizedExpressionForTerm(term));
                }
            }

            regexpBuilder.insert(0, "\\b").append("\\b");

            regularExpression = regexpBuilder.toString();
        }

        return regularExpression;
    }

    public String getUrlString() {
        StringBuilder url = new StringBuilder();

        for (String term : termList) {
            if (url.length() > 0) {
                url.append(" ");
            }

            url.append(term);
        }

        if (proximityCount > 0) {
            url.append("~");
            url.append(proximityCount);
        }

        return url.toString();
    }

    /*--------------------------------------------------------------------------
     *                      PRIVATE METHODS
     */
    private String generalizedExpressionForTerm(String term) {
        return term.replace("*", "(\\S*|\\s*)").replace("?", "\\S").replace("A", "[AÅ]").replace("a", "[aàáãäâā]")
                .replace("ae", "(ae|æ)").replace("c", "[cç]").replace("e", "[eèéë]").replace("i", "[iíï]")
                .replace("n", "[nñ]").replace("o", "[oóôõöø]").replace("u", "[uúü]").replace("1/2", "(1/2|\u00bd)")
                .replace("'", "('|\u2019)");
    }

    private static void reducePossessiveAcronymsInString(StringBuilder string) {
        if (string == null) {
            return;
        }

        Pattern pattern = Pattern.compile("(([a-z]{1,2}[.])+)[']s",
                Pattern.CASE_INSENSITIVE + Pattern.DOTALL + Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(string);

        while (matcher.find()) {
            StringBuilder replacement = new StringBuilder(string.substring(matcher.start(1), matcher.end(1)).replace(",", ""));
            int acronymLength = matcher.end(1) - matcher.start(1);

            while (replacement.length() < acronymLength) {
                replacement.append(" ");
            }

            // NEEDSWORK: should this really be "s "?
            string.replace(matcher.start(), matcher.end(), replacement + "s ");
            matcher = pattern.matcher(string);
        }
    }

    private static void rejoinHyphenatedWordsInString(StringBuilder string) {
        if (string == null) {
            return;
        }

        /*
         * 1 (</span>\\s*)? 2 (<div class=.hyphen.>.*?</div><div class=.break[^>]*>.<a
         * [^>]*>[^<]*<b class=.impdf.></b></a><a [^>]*><b
         * class=.imhbll.></b></a></div>) 3 ( 4 (<[^>]*>)? 5 ([^<\\s]*) 6 (</span>)? )
         */
        Pattern pattern = Pattern.compile(
                "(</span>\\s*)?(<div class=.hyphen.>.*?</div><div class=.break[^>]*>.<a [^>]*>[^<]*"
                        + "<b class=.impdf.></b></a><a [^>]*><b class=.imhbll.></b></a></div>)((<[^>]*>)?([^<\\s]*)(</span>)?)",
                Pattern.CASE_INSENSITIVE + Pattern.DOTALL + Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(string);

        while (matcher.find()) {
            String replacement = string.substring(matcher.start(3), matcher.end(3))
                    + string.substring(matcher.start(2), matcher.end(2));
            int italicsStart = replacement.indexOf(ITALICS_SPAN);

            if (italicsStart >= 0) {
                replacement = string.substring(matcher.start(5), matcher.end(5));

                if (matcher.end(1) > matcher.start(1)) {
                    replacement += string.substring(matcher.start(1), matcher.end(1));
                }

                if (matcher.end(4) > matcher.start(4)) {
                    replacement += string.substring(matcher.start(4), matcher.end(4));
                }

                if (matcher.end(6) > matcher.start(6)) {
                    replacement += string.substring(matcher.start(6), matcher.end(6));
                }

                string.replace(matcher.start(), matcher.end(),
                        replacement + string.substring(matcher.start(2), matcher.end(2)));
            } else {
                string.replace(matcher.start(), matcher.end(), replacement);
            }
        }
    }

    private static void removePunctuation(StringBuilder string) {
        if (string == null) {
            return;
        }

        Pattern pattern = Pattern.compile("(\\d)(,)(([^0-9]|[0-9][^0-9]|[0-9][0-9][^0-9]|[0-9][0-9][0-9][0-9]))",
                Pattern.CASE_INSENSITIVE + Pattern.DOTALL + Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(string);

        while (matcher.find()) {
            string.replace(matcher.start(2), matcher.end(2),
                    String.format("%1$-" + matcher.group(2).length() + "s", " "));
        }

        pattern = Pattern.compile("(\\d\\d\\d\\d)(,)(\\d\\d\\d)",
                Pattern.CASE_INSENSITIVE + Pattern.DOTALL + Pattern.MULTILINE);
        matcher = pattern.matcher(string);

        while (matcher.find()) {
            string.replace(matcher.start(2), matcher.end(2),
                    String.format("%1$-" + matcher.group(2).length() + "s", " "));
        }

        pattern = Pattern.compile("\\b(([\\w-]+://?|www[.])[^\\s()<>]+(?:\\([\\w\\d]+\\)|([^\\p{P}\\p{S}\\s]|/))*)",
                Pattern.CASE_INSENSITIVE + Pattern.DOTALL + Pattern.MULTILINE);
        matcher = pattern.matcher(string);

        while (matcher.find()) {
            String urlMatch = matcher.group(0).replace("/", " ");

            string.replace(matcher.start(0), matcher.end(0), urlMatch);
        }

        pattern = Pattern.compile("([a-zA-Z])(/)", Pattern.CASE_INSENSITIVE + Pattern.DOTALL + Pattern.MULTILINE);
        matcher = pattern.matcher(string);

        while (matcher.find()) {
            string.replace(matcher.start(2), matcher.end(2),
                    String.format("%1$-" + matcher.group(2).length() + "s", " "));
        }

        pattern = Pattern.compile("(/)([a-zA-Z])", Pattern.CASE_INSENSITIVE + Pattern.DOTALL + Pattern.MULTILINE);
        matcher = pattern.matcher(string);

        while (matcher.find()) {
            string.replace(matcher.start(1), matcher.end(1),
                    String.format("%1$-" + matcher.group(1).length() + "s", " "));
        }
    }

    private static void replaceAll(String regularExpression, StringBuilder string) {
        if (string == null || regularExpression == null) {
            return;
        }

        Pattern pattern = Pattern.compile(regularExpression,
                Pattern.CASE_INSENSITIVE + Pattern.DOTALL + Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(string);

        while (matcher.find()) {
            string.replace(matcher.start(), matcher.end(), String.format("%1$-" + matcher.group().length() + "s", " "));
        }
    }

    private static void replaceAll(String regularExpression, String replace, StringBuilder string) {
        if (string == null || regularExpression == null || replace == null) {
            return;
        }

        Pattern pattern = Pattern.compile(regularExpression,
                Pattern.CASE_INSENSITIVE + Pattern.DOTALL + Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(string);

        while (matcher.find()) {
            string.replace(matcher.start(), matcher.end(),
                    String.format("%1$-" + matcher.group().length() + "s", replace));
        }
    }
}
