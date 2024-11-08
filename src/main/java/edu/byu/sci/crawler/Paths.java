package edu.byu.sci.crawler;

import java.io.File;

public class Paths {
    private static final String CACHED_CONTENT_FILE_PREFIX = "contents-";
    private static final String CACHED_CONTENT_FILE_SUFFIX = ".html";
    private static final String CITATIONS_FILE = "citations";
    private static final String EDIT_PATH = "edit";
    private static final String HYPHEN = "-";
    private static final String JSON_EXTENSION = ".json";
    private static final String PAGES_FILE = "pages.txt";
    private static final String PATH_SEPARATOR = "/";
    private static final String REWRITTEN_PATH = "rewritten";
    private static final String TEXT_EXTENSION = ".txt";
    private static final String UPDATED_CITATIONS_FILE = "updated_" + CITATIONS_FILE;

    private String conferencePath;
    private String language;

    public Paths(String conferencePath, String language) {
        this.conferencePath = conferencePath;
        this.language = language;
    }

    public File cachedContentFile(String language) {
        return new File(
                conferencePath + PATH_SEPARATOR + CACHED_CONTENT_FILE_PREFIX + language + CACHED_CONTENT_FILE_SUFFIX);
    }

    public File citationsFile() {
        return new File(conferencePath + PATH_SEPARATOR + CITATIONS_FILE + TEXT_EXTENSION);
    }

    public File conferenceDirectoryFile() {
        return new File(conferencePath);
    }

    public File editedJsonTalkFile(String talkId) {
        return new File(conferencePath + PATH_SEPARATOR + EDIT_PATH + PATH_SEPARATOR + talkId + HYPHEN + language + JSON_EXTENSION);
    }

    public File jsonTalkFile(String talkId) {
        return new File(conferencePath + PATH_SEPARATOR + talkId + HYPHEN + language + JSON_EXTENSION);
    }

    public File languageDirectoryFile() {
        return new File(languagePath());
    }

    public String languagePath() {
        return conferencePath + PATH_SEPARATOR + language + PATH_SEPARATOR;
    }

    public File pagesFile() {
        return new File(conferencePath + PATH_SEPARATOR + PAGES_FILE);
    }

    public File rewrittenDirectoryFile() {
        return new File(languagePath() + REWRITTEN_PATH);
    }

    public File rewrittenTalkFile(String talkId) {
        return new File(languagePath() + REWRITTEN_PATH + PATH_SEPARATOR + talkId);
    }

    public File talkFile(String talkId) {
        return new File(conferencePath + PATH_SEPARATOR + language + PATH_SEPARATOR + talkId);
    }

    public File updatedCitationsFile() {
        return new File(conferencePath + PATH_SEPARATOR + UPDATED_CITATIONS_FILE + "-" + language + TEXT_EXTENSION);
    }
}
