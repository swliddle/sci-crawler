package edu.byu.sci.crawler;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;

public class Books {

    private static final String BOOKS_FILE = "books.json";
    private static final String MAX_CHAPTERS_FILE = "max_chapters.txt";
    private static final String MAX_JST_CHAPTERS_FILE = "max_jst_chapters.txt";

    private JSONArray volumes;
    private Map<Integer, Map<Integer, Integer>> maxChapters = new HashMap<>();
    private Map<Integer, Map<Integer, Integer>> maxJstChapters = new HashMap<>();

    public Books() {
        volumes = new JSONArray(FileUtils.stringFromFile(new File(BOOKS_FILE)));
        readMaxChapters(maxChapters, MAX_CHAPTERS_FILE);
        readMaxChapters(maxJstChapters, MAX_JST_CHAPTERS_FILE);
    }

    public JSONObject bookForAbbreviation(String abbreviation) {
        for (int volumeId = 1; volumeId <= 5; volumeId++) {
            JSONObject volume = volumeForId(volumeId);
            JSONArray books = volume.getJSONArray("books");
            int minBookId = volume.getInt("minBookId");
            int maxBookId = volume.getInt("maxBookId");

            for (int bookId = minBookId; bookId <= maxBookId; bookId++) {
                JSONObject book = books.getJSONObject(bookId - minBookId);

                if (book.getString("abbr").equals(abbreviation)) {
                    return book;
                }
            }
        }

        return null;
    }

    public JSONObject bookForId(int bookId) {
        JSONObject volume = null;
        int minBookId = 0;

        if (bookId >= 101 && bookId <= 139) {
            volume = volumeForId(1);
            minBookId = 101;
        } else if (bookId >= 140 && bookId <= 166) {
            volume = volumeForId(1);
            minBookId = 140;
        } else if (bookId >= 201 && bookId <= 219) {
            volume = volumeForId(1);
            minBookId = 201;
        } else if (bookId >= 301 && bookId <= 303) {
            volume = volumeForId(1);
            minBookId = 301;
        } else if (bookId >= 401 && bookId <= 406) {
            volume = volumeForId(1);
            minBookId = 401;
        }

        if (volume == null) {
            return null;
        }

        return volume.getJSONArray("books").getJSONObject(bookId - minBookId);
    }

    public int maxVerseForBookIdChapter(int bookId, int chapter, boolean isJst) {
        Integer maxVerse = 0;

        if (isJst) {
            maxVerse = maxJstChapters.get(bookId).get(chapter);
        } else {
            maxVerse = maxChapters.get(bookId).get(chapter);
        }

        return maxVerse != null ? maxVerse : 0;
    }

    public JSONObject volumeForId(int volumeId) {
        if (volumeId > 0 && volumeId <= volumes.length()) {
            return volumes.getJSONObject(volumeId - 1);
        }

        return null;
    }

    private void readMaxChapters(Map<Integer, Map<Integer, Integer>> max, String filename) {
        String maxVerseData = FileUtils.stringFromFile(new File(filename));
        String[] lines = maxVerseData.split("\n");

        for (String line : lines) {
            String[] columns = line.split("\t");

            if (columns.length == 3) {
                int bookId = Integer.parseInt(columns[0]);
                int chapter = Integer.parseInt(columns[1]);
                int maxVerse = Integer.parseInt(columns[2]);
                Map<Integer, Integer> chapterMaxMap = max.get(bookId);

                if (chapterMaxMap == null) {
                    chapterMaxMap = new HashMap<>();
                    max.put(bookId, chapterMaxMap);
                }

                chapterMaxMap.put(chapter, maxVerse);
            }
        }
    }
}
