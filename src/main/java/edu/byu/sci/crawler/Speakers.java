package edu.byu.sci.crawler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import org.json.JSONArray;
import org.json.JSONObject;

public class Speakers {

    private static final String SPEAKERS_FILE = "speakers.json";
    private static final String KEY_ABBR = "abbr";
    private static final String KEY_ID = "id";
    private static final String KEY_GIVENNAMES = "givenNames";
    private static final String KEY_LASTNAMES = "lastNames";
    private static final String KEY_SUFFIX = "suffix";
    private static final String KEY_COLLISION = "collision";
    private static final String[] SUFFIXES = {"Jr.", "Sr.", "I", "II", "III", "IV", "V"};

    private final JSONArray speakers;
    private Map<Integer, JSONObject> speakersById = new HashMap<>();
    private Map<String, JSONObject> speakersByAbbr = new HashMap<>();
    private int maxSpeakerId = 0;

    public Speakers() {
        speakers = new JSONArray(FileUtils.stringFromFile(new File(SPEAKERS_FILE)));

        speakers.forEach((item) -> {
            JSONObject speaker = (JSONObject) item;
            int id = speaker.getInt(KEY_ID);

            if (id > maxSpeakerId) {
                maxSpeakerId = id;
            }

            speakersById.put(id, speaker);
            speakersByAbbr.put(speaker.getString(KEY_ABBR), speaker);
        });
    }

    public boolean abbreviationHasCollision(String abbreviation) {
        return speakersByAbbr.keySet().stream()
                .anyMatch(abbr -> (abbr.equalsIgnoreCase(abbreviation)));
    }

    private String abbreviationForName(String givenNames, String lastNames) {
        String[] givenParts = givenNames.split(" ");
        String[] lastParts = lastNames.split(" ");

        if (givenParts.length + lastParts.length <= 3) {
            return abbreviationForParts(Stream.of(givenParts, lastParts).flatMap(Stream::of)
                    .toArray(String[]::new), 3);
        }

        if (lastParts.length < 2 || givenParts.length > 1) {
            return abbreviationForParts(givenParts, 2) + abbreviationForParts(lastParts, 1);
        }

        String abbr = abbreviationForParts(givenParts, 2);

        return abbr + abbreviationForParts(lastParts, 3 - abbr.length());
    }

    private String abbreviationForParts(String[] parts, int maxIndex) {
        String abbr = "";

        for (String part : parts) {
            if (maxIndex > 0) {
                abbr += part.substring(0, 1).toUpperCase();
            }

            maxIndex -= 1;
        }

        return abbr;
    }

    public JSONObject addSpeaker(String name) {
        JSONObject speaker = new JSONObject();

        maxSpeakerId += 1;
        speaker.put(KEY_ID, maxSpeakerId);

        String[] nameParts = parseName(name);

        speaker.put(KEY_GIVENNAMES, nameParts[0]);
        speaker.put(KEY_LASTNAMES, nameParts[1]);
        speaker.put(KEY_SUFFIX, nameParts[2]);
        speaker.put(KEY_ABBR, abbreviationForName(nameParts[0], nameParts[1]));

        if (abbreviationHasCollision(speaker.getString(KEY_ABBR))) {
            speaker.put(KEY_COLLISION, true);
            String abbr = markCollisionsForAbbreviation(speaker.getString(KEY_ABBR));

            speaker.put(KEY_ABBR, abbr);
        } else {
            speaker.put(KEY_COLLISION, false);
        }

        speakers.put(speaker);
        speakersById.put(maxSpeakerId, speaker);
        speakersByAbbr.put(speaker.getString(KEY_ABBR), speaker);

        try {
            FileWriter file = new FileWriter(SPEAKERS_FILE, false);

            speakers.write(file, 4, 0);
            file.close();
        } catch (IOException ex) {
            System.err.println("Error: unable to write speakers file");
        }

        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.out.println("Examine new speaker structure: " + dump(speaker));
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        return speaker;
    }

    private String dump(JSONObject speaker) {
        return "speaker {" + speaker.getInt(KEY_ID)
                + ", " + speaker.getString(KEY_ABBR)
                + ", " + speakerNameLastFirst(speaker)
                + " " + speaker.getBoolean(KEY_COLLISION) + "}";
    }

    public JSONObject exactlyMatchingSpeaker(String name) {
        for (Iterator<Object> it = speakers.iterator(); it.hasNext();) {
            JSONObject speaker = (JSONObject) it.next();

            if (speakerFullName(speaker).equalsIgnoreCase(name)) {
                return speaker;
            }
        }

        return null;
    }

    public JSONObject initialMatchingSpeaker(String name) {
        for (Iterator<Object> it = speakers.iterator(); it.hasNext();) {
            JSONObject speaker = (JSONObject) it.next();
            String[] name1Parts = speakerFullName(speaker).split(" ");
            String[] name2Parts = StringUtils.decodedEntities(name).split(" ");

            if (name1Parts.length == name2Parts.length) {
                boolean couldMatch = true;

                for (int i = 0; couldMatch && i < name1Parts.length; i++) {
                    String name1 = name1Parts[i].replace(".", "");
                    String name2 = name2Parts[i].replace(".", "");

                    if (!name1.equalsIgnoreCase(name2)) {
                        if ((name1.length() > 1 && name2.length() == 1)
                                || (name2.length() > 1 && name1.length() == 1)) {
                            if (!name1.substring(0, 1).equalsIgnoreCase(name2.substring(0, 1))) {
                                couldMatch = false;
                            }
                        } else {
                            couldMatch = false;
                        }
                    }
                }

                if (couldMatch) {
                    return speaker;
                }
            }
        }

        return null;
    }

    private String markCollisionsForAbbreviation(String abbr) {
        JSONArray collisions = new JSONArray();
        String newAbbr;

        for (Object item : speakers) {
            JSONObject speaker = (JSONObject) item;

            if (speaker.getString(KEY_ABBR).equalsIgnoreCase(abbr)) {
                collisions.put(speaker);
            }
        }

        if (abbr.length() < 3) {
            switch (collisions.length()) {
                case 1:
                    newAbbr = abbr.substring(0, 1) + abbr.substring(1, 2).toLowerCase();
                    break;
                case 2:
                    newAbbr = abbr.substring(0, 1).toLowerCase() + abbr.substring(1, 2);
                    break;
                case 3:
                    newAbbr = abbr.toLowerCase();
                    break;
                case 4:
                    newAbbr = abbr + "2";
                    break;
                default:
                    newAbbr = abbr + "3";
                    break;
            }
        } else {
            switch (collisions.length()) {
                case 1:
                    newAbbr = abbr.substring(0, 1) + abbr.substring(1, 2).toLowerCase() + abbr.substring(2, 3);
                    break;
                case 2:
                    newAbbr = abbr.substring(0, 2) + abbr.substring(2, 3).toLowerCase();
                    break;
                case 3:
                    newAbbr = abbr.substring(0, 1).toLowerCase() + abbr.substring(1, 3);
                    break;
                case 4:
                    newAbbr = abbr.toLowerCase();
                    break;
                case 5:
                    newAbbr = abbr.substring(0, 2).toLowerCase() + abbr.substring(2, 3);
                    break;
                case 6:
                    newAbbr = abbr.substring(0, 1).toLowerCase() + abbr.substring(1, 2) + abbr.substring(2, 3).toLowerCase();
                    break;
                default:
                    newAbbr = abbr.substring(0, 1) + abbr.substring(1, 3).toLowerCase();
                    break;
            }
        }

        for (Object item : collisions) {
            JSONObject speaker = (JSONObject) item;

            speaker.put(KEY_COLLISION, true);
        }

        return newAbbr;
    }

    public JSONObject matchingSpeaker(String name) {
        JSONObject speaker = exactlyMatchingSpeaker(name);

        if (speaker == null) {
            speaker = initialMatchingSpeaker(name);

            if (speaker != null) {
                System.err.println("Matching speaker on initials: <" + name
                        + "> to <" + speakerFullName(speaker) + ">");
            }
        }

        return speaker;
    }

    private String[] parseName(String name) {
        String givenNames = "";
        String lastNames = "";
        String suffix = "";

        for (String suffixCandidate : SUFFIXES) {
            if (name.endsWith(" " + suffix) || name.endsWith(", " + suffix)) {
                suffix = suffixCandidate;
                name = name.replaceAll("[,]? " + suffix, "");
            }
        }

        int indexOfDeDiDo = name.toLowerCase().indexOf("de");

        if (indexOfDeDiDo < 0) {
            indexOfDeDiDo = name.toLowerCase().indexOf("di");
        }

        if (indexOfDeDiDo < 0) {
            indexOfDeDiDo = name.toLowerCase().indexOf("do");
        }

        if (indexOfDeDiDo > 0) {
            // This is the cut point
            givenNames = name.substring(0, indexOfDeDiDo - 1);
            lastNames = name.substring(indexOfDeDiDo);
        } else {
            String[] nameParts = name.split(" ");

            if (nameParts.length <= 2) {
                if (nameParts.length > 0) {
                    givenNames = nameParts[0];
                }

                if (nameParts.length > 1) {
                    lastNames = nameParts[1];
                }
            } else {
                for (int i = 0; i < nameParts.length - 1; i++) {
                    if (i > 0) {
                        givenNames += " ";
                    }

                    givenNames += nameParts[i];
                }

                lastNames = nameParts[nameParts.length - 1];
            }
        }

        return new String[]{givenNames, lastNames, suffix};
    }

    public JSONObject speakerForAbbreviation(String abbreviation) {
        return speakersByAbbr.get(abbreviation);
    }

    public JSONObject speakerForId(int speakerId) {
        return speakersById.get(speakerId);
    }

    public static String speakerFullName(JSONObject speaker) {
        String suffix = speaker.getString(KEY_SUFFIX);

        if (suffix.length() > 0) {
            suffix = " " + suffix;
        }

        return StringUtils.decodedEntities(
                speaker.getString(KEY_GIVENNAMES)
                + " " + speaker.getString(KEY_LASTNAMES)
                + suffix
        );
    }

    public static boolean speakerHasCollision(JSONObject speaker) {
        return speaker.getBoolean(KEY_COLLISION);
    }

    public static String speakerNameLastFirst(JSONObject speaker) {
        String suffix = speaker.getString(KEY_SUFFIX);

        if (suffix.length() > 0) {
            if (suffix.startsWith("[")) {
                suffix = " " + suffix;
            } else {
                suffix = ", " + suffix;
            }
        }

        return StringUtils.decodedEntities(
                speaker.getString(KEY_LASTNAMES)
                + ", " + speaker.getString(KEY_GIVENNAMES)
                + suffix
        );
    }
}
