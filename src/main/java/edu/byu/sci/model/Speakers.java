package edu.byu.sci.model;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.Collator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.byu.sci.database.Database;
import edu.byu.sci.util.FileUtils;
import edu.byu.sci.util.StringUtils;

public class Speakers {

    public static final String KEY_ABBR = "abbr";
    public static final String KEY_ID = "id";
    public static final String KEY_GIVENNAMES = "givenNames";
    public static final String KEY_LASTNAMES = "lastNames";
    public static final String KEY_SUFFIX = "suffix";
    public static final String KEY_COLLISION = "collision";

    private static final String SPEAKERS_FILE = "speakers.json";
    private static final String[] SUFFIXES = { "Jr.", "Sr.", "I", "II", "III", "IV", "V" };

    private static final Collator collator = Collator.getInstance(Locale.US);
    private final Logger logger = Logger.getLogger(Speakers.class.getName());
    private final JSONArray speakerRecords;
    private Map<Integer, JSONObject> speakersById = new HashMap<>();
    private Map<String, JSONObject> speakersByAbbr = new HashMap<>();
    private int maxSpeakerId = 0;

    public Speakers(Database database) {
        collator.setStrength(Collator.PRIMARY);

        File speakerFile = new File(SPEAKERS_FILE);

        if (!speakerFile.exists()) {
            speakerRecords = database.speakersFromDatabase();
            FileUtils.writeStringToFile(speakerRecords.toString(4), speakerFile);
        } else {
            speakerRecords = new JSONArray(FileUtils.stringFromFile(new File(SPEAKERS_FILE)));
        }

        speakerRecords.forEach(item -> {
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
        String[] lastParts = lastNames.split("( |\\-)");

        if (givenParts.length + lastParts.length <= 3) {
            return abbreviationForParts(Stream.of(givenParts, lastParts).flatMap(Stream::of)
                    .toArray(String[]::new), 3);
        }

        if (lastParts.length < 2 && givenParts.length > 1) {
            return abbreviationForParts(givenParts, 2) + abbreviationForParts(lastParts, 1);
        }

        if (lastNames.contains("-")) {
            String abbr = abbreviationForParts(lastParts, 2);

            return abbreviationForParts(givenParts, 3 - abbr.length()) + abbr;

        } else {
            String abbr = abbreviationForParts(givenParts, 2);

            return abbr + abbreviationForParts(lastParts, 3 - abbr.length());
        }
    }

    private String abbreviationForParts(String[] parts, int maxIndex) {
        StringBuilder abbr = new StringBuilder();

        for (String part : parts) {
            if (maxIndex > 0) {
                abbr.append(part.substring(0, 1).toUpperCase());
            }

            maxIndex -= 1;
        }

        return abbr.toString();
    }

    public JSONObject addSpeaker(String name) {
        JSONObject speaker = new JSONObject();

        maxSpeakerId += 1;
        speaker.put(KEY_ID, maxSpeakerId);

        String[] nameParts = parseName(name);

        combineSuffixWithSurnames(nameParts);

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

        speakerRecords.put(speaker);
        speakersById.put(maxSpeakerId, speaker);
        speakersByAbbr.put(speaker.getString(KEY_ABBR), speaker);

        try {
            FileWriter file = new FileWriter(SPEAKERS_FILE, false);

            speakerRecords.write(file, 4, 0);
            file.close();
        } catch (JSONException | IOException e) {
            logger.log(Level.SEVERE, "Unable to write JSON file", e);
            System.exit(-1);
        }

        logger.log(Level.WARNING, ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        logger.log(Level.WARNING, () -> "Examine new speaker structure:" + dump(speaker));
        logger.log(Level.WARNING, ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        return speaker;
    }

    private boolean checkMatches(String[] name1Parts, String[] name2Parts) {
        boolean couldMatch = true;

        for (int i = 0; couldMatch && i < name1Parts.length; i++) {
            String name1 = name1Parts[i].replace(".", "");
            String name2 = name2Parts[i].replace(".", "");

            if (collator.compare(name1, name2) != 0) {
                if ((name1.length() > 1 && name2.length() == 1) || (name2.length() > 1 && name1.length() == 1)) {
                    if (collator.compare(name1.substring(0, 1), name2.substring(0, 1)) != 0) {
                        couldMatch = false;
                    }
                } else {
                    couldMatch = false;
                }
            }
        }

        return couldMatch;
    }

    private void combineSuffixWithSurnames(String[] nameParts) {
        String surnames = nameParts[1];
        String suffix = nameParts[2];

        if (!suffix.isEmpty()) {
            if (suffix.contains(".")) {
                surnames += ",";
            }

            nameParts[1] = surnames + " " + suffix;
            nameParts[2] = "";
        }
    }

    private String dump(JSONObject speaker) {
        return "speaker {" + speaker.getInt(KEY_ID)
                + ", " + speaker.getString(KEY_ABBR)
                + ", " + speakerNameLastFirst(speaker)
                + " " + speaker.getBoolean(KEY_COLLISION) + "}";
    }

    public JSONObject exactlyMatchingSpeaker(String name) {
        for (Iterator<Object> it = speakerRecords.iterator(); it.hasNext();) {
            JSONObject speaker = (JSONObject) it.next();

            if (collator.compare(speakerFullName(speaker), name) == 0) {
                return speaker;
            }
        }

        return null;
    }

    public Map<Integer, JSONObject> getSpeakersById() {
        return speakersById;
    }

    public JSONObject initialMatchingSpeaker(String name) {
        for (Iterator<Object> it = speakerRecords.iterator(); it.hasNext();) {
            JSONObject speaker = (JSONObject) it.next();
            String[] name1Parts = speakerFullName(speaker).split(" ");
            String[] name2Parts = StringUtils.decodedEntities(name).split(" ");

            if (name1Parts.length == name2Parts.length && checkMatches(name1Parts, name2Parts)) {
                return speaker;
            }
        }

        return null;
    }

    private String markCollisionsForAbbreviation(String abbr) {
        JSONArray collisions = new JSONArray();
        String newAbbr;

        for (Object item : speakerRecords) {
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
                    newAbbr = abbr.substring(0, 1).toLowerCase() + abbr.substring(1, 2)
                            + abbr.substring(2, 3).toLowerCase();
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
                String fullName = speakerFullName(speaker);

                logger.log(Level.WARNING, () -> "Matching speaker on initials: <" + name
                        + "> to <" + fullName + ">");
            } else {
                speaker = nonSuffixMatchingSpeaker(name);

                if (speaker != null) {
                    String fullName = speakerFullName(speaker);

                    logger.log(Level.WARNING,
                            () -> "Matching speaker without suffix: <" + name
                                    + "> to <" + fullName + ">");
                }
            }
        }
        return speaker;
    }

    public JSONObject nonSuffixMatchingSpeaker(String name) {
        for (Iterator<Object> it = speakerRecords.iterator(); it.hasNext();) {
            JSONObject speaker = (JSONObject) it.next();

            if (speakerFullNameLessSuffix(speaker).equalsIgnoreCase(name)) {
                return speaker;
            }
        }

        return null;
    }

    private String[] parseName(String name) {
        StringBuilder givenNames = new StringBuilder();
        String lastNames = "";
        String suffix = "";

        for (String suffixCandidate : SUFFIXES) {
            if (name.endsWith(" " + suffixCandidate) || name.endsWith(", " + suffixCandidate)) {
                suffix = suffixCandidate;
                name = name.replaceAll("[,]? " + suffix, "");
            }
        }

        int indexOfDeDiDo = name.toLowerCase().indexOf(" de ");

        if (indexOfDeDiDo < 0) {
            indexOfDeDiDo = name.toLowerCase().indexOf(" di ");
        }

        if (indexOfDeDiDo < 0) {
            indexOfDeDiDo = name.toLowerCase().indexOf(" do ");
        }

        if (indexOfDeDiDo > 0) {
            // This is the cut point
            givenNames.append(name.substring(0, indexOfDeDiDo));
            lastNames = name.substring(indexOfDeDiDo);
        } else {
            String[] nameParts = name.split(" ");

            if (nameParts.length <= 2) {
                if (nameParts.length > 0) {
                    givenNames.append(nameParts[0]);
                }

                if (nameParts.length > 1) {
                    lastNames = nameParts[1];
                }
            } else {
                for (int i = 0; i < nameParts.length - 1; i++) {
                    if (i > 0) {
                        givenNames.append(" ");
                    }

                    givenNames.append(nameParts[i]);
                }

                lastNames = nameParts[nameParts.length - 1];
            }
        }

        return new String[] { givenNames.toString(), lastNames, suffix };
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
                        + suffix);
    }

    public static String speakerFullNameLessSuffix(JSONObject speaker) {
        String lastNames = speaker.getString(KEY_LASTNAMES);

        for (String suffix : SUFFIXES) {
            if (lastNames.endsWith(" " + suffix) || lastNames.endsWith(", " + suffix)) {
                lastNames = lastNames.replaceAll("[,]? " + suffix, "");
            }
        }

        return StringUtils.decodedEntities(speaker.getString(KEY_GIVENNAMES) + " " + lastNames);
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
                        + suffix);
    }
}
