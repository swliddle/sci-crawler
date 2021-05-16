package edu.byu.sci.database;

import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.rowset.serial.SerialBlob;

import edu.byu.sci.util.StringUtils;

public class TalkBody {
    private static final String LEFT_BRACKET = " [";
    private static final String MID_BRACKET = "] -> [";
    private static final String RIGHT_BRACKET = "]";

    private final Logger logger = Logger.getLogger(TalkBody.class.getName());

    private String processedText;
    private String rawText;
    private Blob tagVectorBlob;

    public TalkBody(String text) {
        processedText = pageWithCitationPunctuationSubstitutions(text);
        rawText = SearchTerm.cleanHtmlString(processedText).toString();
        processTagVector();
    }

    public String getProcessedText() {
        return processedText;
    }

    public String getRawText() {
        return rawText;
    }

    public Blob getTagVectorBlob() {
        return tagVectorBlob;
    }

    private String leftClassesForString(String punctuation) {
        switch (punctuation) {
            case "(":
                return " lparen";
            case "“":
                return " ldquo";
            case "[":
                return " lbrack";
            case "([":
                return " lparenbrack";
            case "—":
                return " lmdash";
            default:
                return "";
        }
    }

    private class CitationMatch {
        String group1;
        String group2;
        String group3;
        int end;
        int end1;
        int start;
        int start2;

        CitationMatch(Matcher matcher) {
            group1 = matcher.group(1);
            group2 = matcher.group(2);
            group3 = matcher.group(3);
            end = matcher.end();
            end1 = matcher.end(1);
            start = matcher.start();
            start2 = matcher.start(2);
        }
    }
 
    private String pageWithCitationPunctuationSubstitutions(String content) {
        StringBuilder pageBuilder = new StringBuilder(content);
        Pattern pattern = Pattern.compile(
                "(—|[(]|“|\\[|[(]\\[)?<span class=\"citation[^\"]*\"[^>]*><a[^>]*>[^<]*<\\/a><a[^>]*>[^<]*<\\/a><\\/span>([\\)\\]!,.:;?'\"’”“]*)([—]?)",
                Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(pageBuilder);
        List<CitationMatch> matches = new ArrayList<>();

        while (matcher.find()) {
            matches.add(new CitationMatch(matcher));
        }

        for (int i = matches.size() - 1; i >= 0; i--) {
            CitationMatch match = matches.get(i);

            if (!StringUtils.isEmpty(match.group1) || !StringUtils.isEmpty(match.group2)
                    || !StringUtils.isEmpty(match.group3)) {
                String rightClasses = "";
                String leftClasses = "";
                String rightPunctuation = StringUtils.emptyIfNull(match.group2) + StringUtils.emptyIfNull(match.group3);

                if (!StringUtils.isEmpty(match.group1)) {
                    leftClasses = leftClassesForString(match.group1);
                }

                pageBuilder.insert(match.end, "</span>");

                if (rightPunctuation.length() > 0) {
                    rightClasses = rightClassesForString(rightPunctuation, pageBuilder, match.start2);
                    pageBuilder.delete(match.end - rightPunctuation.length(), match.end);
                }

                if (match.end1 > 0) {
                    pageBuilder.delete(match.start, match.end1);
                }

                pageBuilder.insert(match.start, "\">");
                pageBuilder.insert(match.start, rightClasses);
                pageBuilder.insert(match.start, leftClasses);
                pageBuilder.insert(match.start, "<span class=\"ccontainer");
            }
        }

        return pageBuilder.toString();
    }

    private void processTagVector() {
        byte[] tagVector = tagVectorForHtmlString(processedText);

        try {
            tagVectorBlob = new SerialBlob(tagVector);
        } catch (SQLException e) {
            logger.log(Level.SEVERE, () -> "Unable to serialize tag vector: " + e);
        }

        if (rawText.length() != processedText.length() || tagVector.length != rawText.length()) {
            logger.log(Level.WARNING, () -> "Have length issue >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            int processedLen = processedText.length();
            int cleanLen = rawText.length();
            int vectorLen = tagVector.length;
            int maxLen = Math.min(cleanLen, processedLen);
            maxLen = Math.min(maxLen, vectorLen);

            for (int i = 0; i < maxLen; i++) {
                final int index = i;
                char pc = processedText.charAt(i);
                char cc = rawText.charAt(i);

                if (tagVector[i] == 0) {
                    if (cc == ' ') {
                        if (Character.isAlphabetic(pc) || Character.isDigit(pc)) {
                            logger.log(Level.WARNING, () -> "Suspicious character " + index + LEFT_BRACKET + pc
                                    + MID_BRACKET + cc + "] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
                        } else {
                            logger.log(Level.INFO, () -> "Subsuming character " + index + LEFT_BRACKET + pc
                                    + MID_BRACKET + cc + RIGHT_BRACKET);
                        }
                    } else {
                        if (Character.toLowerCase(pc) == cc) {
                            logger.log(Level.INFO,
                                    () -> "Character " + index + LEFT_BRACKET + pc + MID_BRACKET + cc + RIGHT_BRACKET);
                        } else {
                            logger.log(Level.WARNING, () -> "Suspicious character " + index + LEFT_BRACKET + pc
                                    + MID_BRACKET + cc + "] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
                        }
                    }
                } else if (tagVector[i] == 1) {
                    logger.log(Level.INFO,
                            () -> "Tag character " + index + LEFT_BRACKET + pc + MID_BRACKET + cc + RIGHT_BRACKET);
                } else {
                    logger.log(Level.WARNING, () -> "Unexpected tagVector value (" + tagVector[index]
                            + ") <*><*><*><*><*><*><*><*><*><*><*><*><*><*><*>");
                }
            }
        }
    }

    private String rightClassesForString(String punctuation, StringBuilder content, int pos) {
        StringBuilder rightClasses = new StringBuilder(" r");

        final int length = punctuation.length();

        for (int offset = 0; offset < length;) {
            final int codepoint = punctuation.codePointAt(offset);
            String classToAdd = "";

            switch (codepoint) {
                case '!':
                    classToAdd = "bang";
                    break;
                case ',':
                    classToAdd = "comma";
                    break;
                case '.':
                    classToAdd = "dot";
                    break;
                case ':':
                    classToAdd = "colon";
                    break;
                case ';':
                    classToAdd = "semi";
                    break;
                case '?':
                    classToAdd = "quest";
                    break;
                case '\'':
                    classToAdd = "apos";
                    break;
                case '"':
                    classToAdd = "quote";
                    break;
                case '’':
                    classToAdd = "rsquo";
                    break;
                case '“':
                    classToAdd = "ldquo";
                    break;
                case '”':
                    classToAdd = "rdquo";
                    break;
                case ')':
                    classToAdd = "paren";
                    break;
                case ']':
                    classToAdd = "brack";
                    break;
                case '—':
                    classToAdd = "mdash";
                    break;
                case '…':
                    classToAdd = "ellip";
                    break;
                default:
                    terminateWithWarning(content, punctuation, codepoint, pos);
                    break;
            }

            rightClasses.append(classToAdd);
            offset += Character.charCount(codepoint);
        }

        if (rightClasses.length() <= 2) {
            logger.log(Level.SEVERE, () -> "Warning: found no right classes for " + punctuation);

            final int punctuationLength = punctuation.length();

            for (int offset = 0; offset < punctuationLength;) {
                final int codepoint = punctuation.codePointAt(offset);

                logger.log(Level.SEVERE, () -> "Codepoint " + codepoint);
                offset += Character.charCount(codepoint);
            }

            System.exit(-1);
        }

        return rightClasses.toString();
    }

    public byte[] tagVectorForHtmlString(String string) {
        int length = string.length();
        byte[] vector = new byte[length];
        boolean inTag = false;

        for (int i = 0; i < length; i++) {
            char c = string.charAt(i);

            if (c == '<') {
                inTag = true;
                vector[i] = 1;
            } else if (c == '>') {
                inTag = false;
                vector[i] = 1;
            } else if (inTag) {
                vector[i] = 1;
            } else {
                vector[i] = 0;
            }
        }

        return vector;
    }

    private void terminateWithWarning(StringBuilder content, String punctuation, int value, int pos) {
        final int start = pos > 10 ? pos - 10 : pos;

        logger.log(Level.WARNING, () -> "Punctuation " + value + " not mapped at position " + pos);
        logger.log(Level.WARNING, () -> "Context is " + punctuation);
        logger.log(Level.WARNING, () -> "Source is " + content.substring(start));

        System.exit(-1);
    }
}
