package edu.byu.sci.crawler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;
// import java.util.ArrayList;
// import java.util.Collections;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Verses {

    private static final String VERSES_FILE = "verses.json";

    private JSONArray verseRecords;

    public Verses() {
        verseRecords = new JSONArray(FileUtils.stringFromFile(new File(VERSES_FILE)));

        verseRecords.forEach(item -> {
            JSONObject verse = (JSONObject) item;

            if (verse.getString("text").contains("<")) {
                if (!verse.has("type") || !verse.getString("type").equals("html")) {
                    Logger.getLogger(Verses.class.getName()).log(Level.WARNING, "Verse with HTML not marked");
                }
            } else {
                if (verse.has("type") && verse.getString("type").equals("html")) {
                    Logger.getLogger(Verses.class.getName()).log(Level.WARNING, "Verse marked HTML but has no tags");
                }
            }

            // if (verse.getString("flag").equals("H")) {
            //     verse.put("className", "headnote");
            // }
        });

        // ArrayList<JSONObject> list = new ArrayList<>();

        // int len = verseRecords.length();

        // for (int i = 0; i < len; i++) {
        //     list.add((JSONObject) verseRecords.get(i));
        // }

        // Collections.sort(list, (a, b) -> {
        //     try {
        //         int result = a.getInt("id") - b.getInt("id");

                // if (result == 0) {
                //     result = a.getInt("chapter") - b.getInt("chapter");

                //     if (result == 0) {
                //         result = a.getInt("verse") - b.getInt("verse");
                        
                //         if (result == 0) {
                //             result = b.getString("flag").compareTo(a.getString("flag"));
                //         }
                //     }
                // }

        //         return result;
        //     } catch (JSONException e) {
        //         // Ignore
        //         return 0;
        //     }
        // });

        // verseRecords = new JSONArray();

        // list.forEach(item -> {
        //     verseRecords.put(item);
        // });

        try {
            FileWriter file = new FileWriter("verses2.json", false);

            verseRecords.write(file, 4, 0);
            file.close();
        } catch (JSONException | IOException e) {
            // Ignore
        }
    }

    public int count() {
        return verseRecords.length();
    }
}
