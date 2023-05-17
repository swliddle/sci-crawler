package edu.byu.sci.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.byu.sci.model.BookFinder;
import edu.byu.sci.model.Link;
import edu.byu.sci.model.Speakers;
import edu.byu.sci.util.StringUtils;
import edu.byu.sci.util.Utils;

public class Database {
    private class DatabaseException extends RuntimeException {
        public DatabaseException(String description) {
            super(description);
        }
    }

    private static final Map<String, String> sConferenceDescription = new HashMap<>();
    private static final Map<String, String> sConferenceAbbreviation = new HashMap<>();
    private static final Map<String, String> sSessionAbbreviation = new HashMap<>();

    private static final String FIELD_VERSES = "Verses";

    private static final String INSERT_INTO = "INSERT INTO ";
    private static final String REPLACE_INTO = "REPLACE INTO ";

    private static final String MEDIA_URL_PREFIX = "https://media2.ldscdn.org/assets/";
    private static final String TALK_URL_PREFIX = "https://www.churchofjesuschrist.org";

    private static final int POLYMORPHIC_CTYPE_ID_EN = 31;
    private static final int POLYMORPHIC_CTYPE_ID_ES = 62;

    private static final String[] SESSION_TITLES_EN = { "priesthood", "women&rsquo;s", "sunday morning",
            "sunday afternoon", "saturday morning", "saturday afternoon", "saturday evening" };

    private static final String[] SESSION_TITLES_ES = { "del sacerdocio", "general de mujeres",
            "del domingo por la mañana", "del domingo por la tarde", "del sábado por la mañana",
            "del sábado por la tarde", "del sábado por la noche" };

    private static final String TABLE_CITATION = "citation";
    private static final String TABLE_CITATION_VERSE = "citation_verse";
    private static final String TABLE_CONFERENCE = "conference";
    private static final String TABLE_CONFERENCE_SESSION = "conf_session";
    private static final String TABLE_CONFERENCE_TALK = "conference_talk";
    private static final String TABLE_SPEAKER = "speaker";
    private static final String TABLE_TALK = "talk";
    private static final String TABLE_TALK_BODY = "talkbody";
    private static final String TABLE_TALK_BODY2 = TABLE_TALK_BODY + "2";
    private static final String TABLE_TALK_STREAM = "talk_stream";

    private Connection conn = null;
    private Logger logger = Logger.getLogger(Database.class.getName());
    private Map<Integer, Integer> sessionIds = new HashMap<>();
    private Map<Integer, String> sessionDates = new HashMap<>();
    private Map<String, Integer> talkIdsForKeys = new HashMap<>();

    public Database() {
        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3307/sci2p?user=sci2puser&password=sci44access");
            // conn =
            // DriverManager.getConnection("jdbc:mysql://localhost/sci3?user=sci3user&password=vt43by8c89nvn3");
        } catch (SQLException e) {
            logError(e);
        }

        sConferenceDescription.put("A-eng", " Annual General Conference");
        sConferenceDescription.put("A-spa", " Conferencia General Anual");
        sConferenceDescription.put("S-eng", " Semi-Annual General Conference");
        sConferenceDescription.put("S-spa", " Conferencia General Semestral");

        sConferenceAbbreviation.put("A-eng", " Annual");
        sConferenceAbbreviation.put("A-spa", " Anual");
        sConferenceAbbreviation.put("S-eng", " Semi-Annual");
        sConferenceAbbreviation.put("S-spa", " Semestral");

        sSessionAbbreviation.put("priesthood", "Priesthood Session");
        sSessionAbbreviation.put("women&rsquo;s", "Women&rsquo;s Session");
        sSessionAbbreviation.put("sunday morning", "Sunday Morning");
        sSessionAbbreviation.put("sunday afternoon", "Sunday Afternoon");
        sSessionAbbreviation.put("saturday morning", "Saturday Morning");
        sSessionAbbreviation.put("saturday afternoon", "Saturday Afternoon");
        sSessionAbbreviation.put("saturday evening", "Saturday Evening");
        sSessionAbbreviation.put("del sacerdocio", "Sesión del Sacerdocio");
        sSessionAbbreviation.put("general de mujeres", "Sesión de Mujeres");
        sSessionAbbreviation.put("del domingo por la mañana", "Domingo por la Mañana");
        sSessionAbbreviation.put("del domingo por la tarde", "Domingo por la Tarde");
        sSessionAbbreviation.put("del sábado por la mañana", "Sábado por la Mañana");
        sSessionAbbreviation.put("del sábado por la tarde", "Sábado por la Tarde");
        sSessionAbbreviation.put("del sábado por la noche", "Sábado por la Noche");
    }

    private String abbreviationForSession(String session, String language) {
        String[] sessionTitles = language.equals("spa") ? SESSION_TITLES_ES : SESSION_TITLES_EN;
        String lowerSession = session.toLowerCase();

        for (String title : sessionTitles) {
            if (lowerSession.contains(title)) {
                String abbreviation = sSessionAbbreviation.get(title);

                if (abbreviation != null) {
                    return abbreviation;
                }
            }
        }

        return session;
    }

    private int addCitation(Link citation, List<Link> citations, int maxCitationId) {
        maxCitationId += 1;
        citation.citationId = maxCitationId;
        citations.add(citation);

        return maxCitationId;
    }

    public void calculateVerseSets() {
        logger.log(Level.INFO, "Calculate verse sets");
        int max = 0;
        int min = 0;
        boolean first;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.createStatement();
            String sql = "SELECT ID, Verses FROM " + TABLE_CITATION + " WHERE MinVerse=0 OR MaxVerse=0";
            String updateSql = REPLACE_INTO + TABLE_CITATION_VERSE + " (CitationID, Verse) VALUES (";

            stmt.execute(sql);
            rs = stmt.getResultSet();

            while (rs.next()) {
                String versesField = rs.getString(FIELD_VERSES);

                max = 0;
                first = true;

                if (versesField == null || versesField.trim().equals("") || versesField.equalsIgnoreCase("headnote")) {
                    // Skip citations that have no verses.
                    continue;
                }

                String[] verseRanges = rs.getString(FIELD_VERSES).split(",");

                for (String range : verseRanges) {
                    String[] verses = range.split("-");
                    int verse1 = Integer.parseInt(verses[0]);
                    int verse2 = verse1;

                    if (first) {
                        min = verse1;
                        first = false;
                    }

                    if (verses.length == 2) {
                        verse2 = Integer.parseInt(verses[1]);
                    }

                    for (int i = verse1; i <= verse2; i++) {
                        if (i > max) {
                            max = i;
                        }

                        executeSql(updateSql + rs.getString("ID") + ", " + i + ")");
                    }
                }

                executeVerseUpdate("MinVerse", min, rs.getString("ID"));
                executeVerseUpdate("MaxVerse", max, rs.getString("ID"));
            }
        } catch (SQLException e) {
            logError(e);
        } finally {
            cleanupStatement(stmt, rs);
        }
    }

    private GregorianCalendar chooseSessionDate(String session, String language, GregorianCalendar saturdayDate,
            GregorianCalendar sundayDate) {

        String sundayLabel = "Sunday";

        if (language.equals("spa")) {
            sundayLabel = "Domingo";
        }

        if (session.toLowerCase().contains(sundayLabel.toLowerCase())) {
            return sundayDate;
        }

        return saturdayDate;
    }

    private List<Link> citationsForTalk(int talkId) {
        Statement stmt = null;
        ResultSet rs = null;
        List<Link> citations = new ArrayList<>();

        try {
            stmt = conn.createStatement();
            // citation:
            // ID TalkID BookID Chapter Verses Flag PageColumn MinVerse MaxVerse
            rs = stmt.executeQuery("SELECT * FROM citation WHERE TalkID=" + talkId + " ORDER BY ID");

            while (rs.next()) {
                Link link = new Link(null, null, null);

                link.citationId = rs.getInt(1);
                link.talkId = rs.getString(2);
                link.book = rs.getString(3);
                link.chapter = rs.getString(4);
                link.verses = rs.getString(5);
                link.isJst = "J".equals(rs.getString(6));
                link.page = Utils.integerValue(rs.getString(7));

                citations.add(link);
            }
        } catch (SQLException e) {
            logError(e);
        } finally {
            cleanupStatement(stmt, rs);
        }

        return citations;
    }

    public void cleanupStatement(Statement stmt, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException sqlEx) {
                // ignore
            }
        }

        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException sqlEx) {
                // ignore
            }
        }
    }

    private String conferenceAbbreviation(int year, String language, String annual) {
        return year + sConferenceAbbreviation.get(annual + "-" + language);
    }

    private String conferenceDescription(int year, String language, String annual) {
        return year + sConferenceDescription.get(annual + "-" + language);
    }

    private void createTalkbody2Tables() {
        String prefix = "CREATE TABLE IF NOT EXISTS `";
        String suffix = "` (`TalkID` int(11) NOT NULL, `Text` mediumtext, "
                + "`ProcessedText` mediumtext, `RawText` mediumtext, `TagVector` mediumblob, "
                + "PRIMARY KEY (`TalkID`)) ENGINE=MyISAM DEFAULT CHARSET=utf8";

        executeSql(prefix + "talkbody2" + suffix);
        executeSql(prefix + "talkbody2_es" + suffix);
    }

    private String descriptionForSession(String session, GregorianCalendar date, String language) {
        DateFormat dateFormat = new SimpleDateFormat("d MMM yyyy",
                language.equals("spa") ? new Locale("es") : Locale.US);

        return session + ", " + dateFormat.format(date.getTime());
    }

    private void executeSql(String sql) {
        Statement stmt = null;

        try {
            stmt = conn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            logError(e);
        } finally {
            cleanupStatement(stmt, null);
        }
    }

    private void executeVerseUpdate(String field, int value, String id) {
        Statement stmt = null;

        try {
            stmt = conn.createStatement();
            stmt.executeUpdate("UPDATE " + TABLE_CITATION + " SET " + field + "=" + value + " WHERE ID='" + id + "'");
        } catch (SQLException e) {
            logError(e);
        } finally {
            cleanupStatement(stmt, null);
        }
    }

    private int existingConferenceId(int year, String annual, String issueDate, String language) {
        Statement stmt = null;
        ResultSet rs = null;
        int conferenceId = -1;

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT ID FROM " + tableForLanguage(TABLE_CONFERENCE, language) + " WHERE YEAR = '"
                    + year + "' AND Annual='" + annual + "' AND IssueDate='" + issueDate + "'");

            if (rs.next()) {
                conferenceId = rs.getInt(1);
            }
        } catch (SQLException e) {
            logError(e);
        } finally {
            cleanupStatement(stmt, rs);
        }

        return conferenceId;
    }

    private int existingSessionId(String sessionDate, int sequence, String language) {
        Statement stmt = null;
        ResultSet rs = null;
        int sessionId = -1;

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(
                    "SELECT ID FROM " + tableForLanguage(TABLE_CONFERENCE_SESSION, language)
                            + " WHERE Date = '" + sessionDate + "' AND Sequence='" + sequence + "'");

            if (rs.next()) {
                sessionId = rs.getInt(1);
            }
        } catch (SQLException e) {
            logError(e);
        } finally {
            cleanupStatement(stmt, rs);
        }

        return sessionId;
    }

    private int existingTalkId(String title, String date, int sequence, String language) {
        Statement stmt = null;
        ResultSet rs = null;
        int talkId = -1;

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT TalkID FROM " + TABLE_CONFERENCE_TALK + " ct JOIN "
                    + tableForLanguage(TABLE_TALK, language) + " t WHERE t.ID = ct.TalkID AND t.title = '" + title
                    + "' AND t.Date = '" + date + "' AND ct.Sequence='" + sequence + "'");

            if (rs.next()) {
                talkId = rs.getInt(1);
            }
        } catch (SQLException e) {
            logError(e);
        } finally {
            cleanupStatement(stmt, rs);
        }

        return talkId;
    }

    private int existingTalkStreamId(int talkId, String language) {
        Statement stmt = null;
        ResultSet rs = null;
        int existingTalkId = -1;

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(
                    "SELECT TalkID FROM " + tableForLanguage(TABLE_TALK_STREAM, language) + " WHERE TalkID=" + talkId);

            if (rs.next()) {
                existingTalkId = rs.getInt(1);
            }
        } catch (SQLException e) {
            logError(e);
        } finally {
            cleanupStatement(stmt, rs);
        }

        return existingTalkId;
    }

    private String fullTalkUrl(String url) {
        if (!url.startsWith("http")) {
            return TALK_URL_PREFIX + url;
        }

        return url;
    }

    public Connection getConnection() {
        return conn;
    }

    public int getMaxCitationId() {
        return getMaxId(TABLE_CITATION);
    }

    private int getMaxId(String tableName) {
        Statement stmt = null;
        ResultSet rs = null;
        int maxId = -1;

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT MAX(ID) FROM " + tableName);
            rs.next();

            maxId = rs.getInt(1);
        } catch (SQLException e) {
            logError(e);
        } finally {
            cleanupStatement(stmt, rs);
        }

        return maxId;
    }

    private int getMaxTalkId(String tableName) {
        Statement stmt = null;
        ResultSet rs = null;
        int maxId = -1;

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT MAX(ID) FROM " + tableName + " WHERE ID < 10000");
            rs.next();

            maxId = rs.getInt(1);
        } catch (SQLException e) {
            logError(e);
        } finally {
            cleanupStatement(stmt, rs);
        }

        return maxId;
    }

    private int indexOfMatch(List<Link> citations, Link citation) {
        for (int i = 0; i < citations.size(); i++) {
            Link candidate = citations.get(i);

            if (linksMatch(citation, candidate)) {
                return i;
            }
        }

        return -1;
    }

    private boolean linksMatch(Link link1, Link link2) {
        String candidateTalkId = "" + talkIdsForKeys.get(link1.talkId);
        String candidateBookId = "" + BookFinder.sInstance.bookIdForBook(link1.book);

        return link2.talkId.equals(candidateTalkId) && link2.book.equals(candidateBookId)
                && link2.chapter.equals(link1.chapter) && link2.verses.equals(link1.verses) && link2.page == link1.page
                && link2.isJst == link1.isJst;
    }

    private void logCitation(Link citation) {
        logger.log(Level.INFO,
                () -> "Need to write citation: " + citation.citationId + " " + citation.talkId + " " + citation.book
                        + " " + citation.chapter + " " + citation.verses + " " + citation.isJst + " " + citation.page);
    }

    public void logError(SQLException e) {
        logger.log(Level.SEVERE, () -> "SQLException: " + e.getMessage());
        logger.log(Level.SEVERE, () -> "SQLState: " + e.getSQLState());
        logger.log(Level.SEVERE, () -> "VendorError: " + e.getErrorCode());
        System.exit(-1);
    }

    /**
     * Execute an SQL query statement and iterate over the corresponding
     * {@code ResultSet}, calling the {@code QueryResponder} for each result record.
     *
     * @param sql Query to execute.
     * @param qr  Callback object for processing each record in the
     *            {@code ResultSet}.
     */
    public void query(String sql, QueryResponder qr) {
        Statement stmt = null;
        ResultSet rs = null;

        logger.log(Level.INFO, () -> "query " + sql);
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);

            while (rs.next()) {
                qr.processResult(rs);
            }
        } catch (SQLException se) {
            logger.log(Level.SEVERE, () -> "Unable to process query (" + sql + "):\r\n" + se);
        } finally {
            cleanupStatement(stmt, rs);
        }
    }

    private boolean speakerHasCollision(JSONObject speaker, JSONArray speakers) {
        for (int i = 0; i < speakers.length(); i++) {
            JSONObject candidate = speakers.getJSONObject(i);

            if (speaker.getString("abbr").equalsIgnoreCase(candidate.getString("abbr"))) {
                return true;
            }
        }

        return false;
    }

    public JSONArray speakersFromDatabase() {
        Statement stmt = null;
        ResultSet rs = null;
        JSONArray speakers = new JSONArray();

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT ID, GivenNames, LastNames, Abbr, Info FROM speaker ORDER BY ID ASC");

            while (rs.next()) {
                JSONObject speaker = new JSONObject();

                speaker.put("id", rs.getInt(1));
                speaker.put("givenNames", rs.getString(2));
                speaker.put("lastNames", rs.getString(3));
                speaker.put("abbr", rs.getString(4));

                if (rs.getString(5) != null) {
                    speaker.put("suffix", rs.getString(5));
                } else {
                    speaker.put("suffix", "");
                }

                speaker.put("collision", speakerHasCollision(speaker, speakers));

                speakers.put(speaker);
            }
        } catch (SQLException e) {
            logError(e);
        } finally {
            cleanupStatement(stmt, rs);
        }

        return speakers;
    }

    private String tableForLanguage(String table, String language) {
        return language.equals("spa") ? table + "_es" : table;
    }

    private Map<String, Integer> talksForConference(int year, String annual) {
        Statement stmt = null;
        ResultSet rs = null;
        Map<String, Integer> talkIds = new HashMap<>();

        try {
            stmt = conn.createStatement();
            // citation:
            // ID TalkID BookID Chapter Verses Flag PageColumn MinVerse MaxVerse
            rs = stmt.executeQuery("SELECT t.ID, t.URL FROM talk t "
                    + "JOIN conference_talk ct JOIN conf_session cs JOIN conference c "
                    + "WHERE t.ID=ct.TalkID AND ct.SessionID=cs.ID AND "
                    + "cs.ConferenceID=c.ID AND c.year=" + year + " AND c.Annual='" + annual + "'");

            while (rs.next()) {
                int talkId = rs.getInt(1);
                String talkKey = rs.getString(2);
                int index = talkKey.indexOf("?");

                if (index >= 0) {
                    talkKey = talkKey.substring(0, index);
                }

                index = talkKey.lastIndexOf("/");

                if (index >= 0 && index + 1 < talkKey.length()) {
                    talkKey = talkKey.substring(index + 1);
                }

                talkIds.put(talkKey, talkId);
                String key = talkKey;
                logger.log(Level.INFO, () -> "Talk ID map: " + key + " -> " + talkId);
            }
        } catch (SQLException e) {
            logError(e);
        } finally {
            cleanupStatement(stmt, rs);
        }

        return talkIds;
    }

    private String trimMediaUrl(String url) {
        if (url.startsWith(MEDIA_URL_PREFIX)) {
            return url.substring(MEDIA_URL_PREFIX.length());
        }

        return url;
    }

    /*
     * Strategy is to start by querying the talks for this conference.
     * Create a map from talkId (e.g. 11nelson) to TalkID (e.g. 8057).
     * Then create a map from talkId to set of citations for that talk.
     * Without talks already in the database, we can't add citations.
     */
    public void updateCitations(boolean writeToDatabase, List<Link> citations, List<String> talkIds, int year,
            String annual, String language) {
        Map<String, Integer> talkIdsMap = talksForConference(year, annual);

        if (talkIdsMap.isEmpty()) {
            int maxTalkId = getMaxTalkId(tableForLanguage(TABLE_TALK, language));

            for (String talkId : talkIds) {
                maxTalkId += 1;
                talkIdsMap.put(talkId, maxTalkId);
            }
        }

        // Cases: no citations, some citations
        for (String talkId : talkIds) {
            Link[] talkCitations = citations.stream().filter(citation -> citation.talkId.equals(talkId))
                    .toArray(Link[]::new);

            if (talkIdsMap.get(talkId) == null) {
                logger.log(Level.SEVERE, () -> "Null map for " + talkId);

                talkIdsMap.forEach((key, value) -> logger.log(Level.SEVERE, () -> key + ": " + value));
            }

            updateCitationsForTalk(writeToDatabase, talkId, talkIdsMap.get(talkId), talkCitations);
        }

        calculateVerseSets();
    }

    private void updateCitationsForTalk(boolean writeToDatabase, String talkId, int talkIdValue,
            Link[] citations) {
        if (citations.length <= 0) {
            // By definition there is nothing to do
            return;
        }

        int maxCitationId = getMaxId(TABLE_CITATION);
        List<Link> talkCitations = citationsForTalk(talkIdValue);
        List<Link> citationsToWrite = new ArrayList<>();

        if (talkCitations.isEmpty()) {
            // There are no citations in the database
            for (Link citation : citations) {
                maxCitationId = addCitation(citation, citationsToWrite, maxCitationId);
            }
        } else {
            // We need to compare to see if we have the same citations...
            for (Link citation : citations) {
                // Can we find this citation in the database list?
                int index = indexOfMatch(talkCitations, citation);

                if (index >= 0) {
                    logger.log(Level.INFO, () -> "Already have citation record for " + citation.citationId);
                    citation.citationId = talkCitations.get(index).citationId;
                    talkCitations.remove(index);
                } else {
                    logger.log(Level.INFO, () -> "Need to create citation record for " + citation.citationId);
                    maxCitationId = addCitation(citation, citationsToWrite, maxCitationId);
                }
            }

            if (citationsToWrite.isEmpty()) {
                logger.log(Level.INFO, () -> "There are no citations to write for talk " + talkId);
            }

            if (!talkCitations.isEmpty()) {
                logger.log(Level.WARNING, () -> "There are citations in database not in list for talk " + talkId);
            }
        }

        citationsToWrite.forEach(this::logCitation);

        if (writeToDatabase) {
            try {
                writeCitationsToDatabase(citationsToWrite);
            } catch (SQLException e) {
                logError(e);
            }
        }
    }

    private int updateConference(boolean writeToDatabase, String description, String abbr, int year, String annual,
            String issueDate, String language) throws DatabaseException {

        PreparedStatement stmt = null;
        ResultSet rs = null;
        int conferenceId = existingConferenceId(year, annual, issueDate, language);

        if (writeToDatabase) {
            if (conferenceId < 0) {
                logger.log(Level.INFO, "Inserting a conference record");

                try {
                    stmt = conn.prepareStatement(
                            INSERT_INTO + tableForLanguage(TABLE_CONFERENCE, language)
                                    + " (Description, Abbr, Year, Annual, IssueDate) " + "VALUES (?, ?, ?, ?, ?)",
                            Statement.RETURN_GENERATED_KEYS);
                    stmt.setString(1, description);
                    stmt.setString(2, abbr);
                    stmt.setInt(3, year);
                    stmt.setString(4, annual);
                    stmt.setString(5, issueDate);
                    stmt.execute();
                    rs = stmt.getGeneratedKeys();

                    if (rs.next()) {
                        conferenceId = rs.getInt(1);
                    } else {
                        throw new DatabaseException("Unable to retrieve generated conference ID");
                    }
                } catch (SQLException e) {
                    logError(e);
                } finally {
                    cleanupStatement(stmt, rs);
                }
            } else {
                logger.log(Level.INFO, "Skip insert: it appears there is already a conference record");
            }
        } else {
            if (conferenceId < 0) {
                conferenceId = getMaxId(tableForLanguage(TABLE_CONFERENCE, language)) + 1;
                logger.log(Level.INFO, "Need to insert conference record");
            } else {
                logger.log(Level.INFO, "It appears there is already a conference record");
            }
        }

        final int id = conferenceId;

        logger.log(Level.INFO, () -> "Conference: " + id + "; " + description + "; " + abbr + "; " + year
                + "; " + annual + "; " + issueDate);

        return conferenceId;
    }

    public void updateMetaData(boolean writeToDatabase, int year, String language, String annual, String issueDate,
            List<String> sessions, GregorianCalendar saturdayDate, GregorianCalendar sundayDate,
            List<String> talkIds, Map<String, String> talkHrefs, Map<String, Integer> talkSpeakerIds,
            Map<String, String> talkTitles, Map<String, int[]> pageRanges, Map<String, Integer> talkSequence,
            Map<String, Integer> talkSessionNo, Map<String, String> talkAudioUrls,
            Map<String, String[]> talkVideoUrls) {

        createTalkbody2Tables();
        int conferenceId = updateConference(writeToDatabase, conferenceDescription(year, language, annual),
                conferenceAbbreviation(year, language, annual), year, annual, issueDate, language);
        updateSessions(writeToDatabase, sessions, language, saturdayDate, sundayDate, conferenceId);
        updateTalks(writeToDatabase, talkIds, talkHrefs, talkSpeakerIds, talkTitles, pageRanges, talkSequence,
                talkSessionNo, talkAudioUrls, talkVideoUrls, language);
    }

    private int updateSession(boolean writeToDatabase, String description, String abbreviation,
            String sessionDate, int sequence, int conferenceId, String language) {

        PreparedStatement stmt = null;
        ResultSet rs = null;
        int sessionId = existingSessionId(sessionDate, sequence, language);

        if (writeToDatabase) {
            if (sessionId < 0) {
                logger.log(Level.INFO, "Inserting a conference record");

                try {
                    stmt = conn.prepareStatement(
                            INSERT_INTO + tableForLanguage(TABLE_CONFERENCE_SESSION, language)
                                    + " (Description, Abbr, Date, Sequence, ConferenceID) " + "VALUES (?, ?, ?, ?, ?)",
                            Statement.RETURN_GENERATED_KEYS);
                    stmt.setString(1, description);
                    stmt.setString(2, abbreviation);
                    stmt.setString(3, sessionDate);
                    stmt.setInt(4, sequence);
                    stmt.setInt(5, conferenceId);
                    stmt.execute();
                    rs = stmt.getGeneratedKeys();

                    if (rs.next()) {
                        sessionId = rs.getInt(1);
                    } else {
                        throw new DatabaseException("Unable to retrieve generated conference session ID");
                    }
                } catch (SQLException e) {
                    logError(e);
                } finally {
                    cleanupStatement(stmt, rs);
                }
            } else {
                logger.log(Level.INFO, "Skip insert: it appears there is already a conference session record");
            }
        } else {
            if (sessionId < 0) {
                sessionId = getMaxId(tableForLanguage(TABLE_CONFERENCE_SESSION, language)) + sequence;
                logger.log(Level.INFO, "Need to insert conference session record");
            } else {
                logger.log(Level.INFO, "It appears there is already a conference session record");
            }
        }

        final int id = sessionId;

        logger.log(Level.INFO, () -> "Conference Session: " + id + "; " + description + "; " + abbreviation + "; "
                + sessionDate + "; " + sequence + "; " + conferenceId);

        return sessionId;
    }

    private void updateSessions(boolean writeToDatabase, List<String> sessions, String language,
            GregorianCalendar saturdayDate, GregorianCalendar sundayDate, int conferenceId) {
        for (int i = 0; i < sessions.size(); i++) {
            String session = sessions.get(i);
            GregorianCalendar date = chooseSessionDate(session, language, saturdayDate, sundayDate);
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String sessionDate = dateFormat.format(date.getTime());
            String description = descriptionForSession(session, date, language);
            String abbreviation = abbreviationForSession(session, language);

            int sessionId = updateSession(writeToDatabase, description, abbreviation, sessionDate, i + 1, conferenceId,
                    language);

            sessionIds.put(i + 1, sessionId);
            sessionDates.put(i + 1, sessionDate);
        }
    }

    public void updateSpeakers(boolean writeToDatabase, Speakers speakers) {
        Statement stmt = null;
        ResultSet rs = null;
        Map<Integer, String> databaseSpeakers = new HashMap<>();

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT ID, Abbr FROM " + TABLE_SPEAKER);

            while (rs.next()) {
                int speakerId = rs.getInt(1);
                String speakerAbbr = rs.getString(2);

                databaseSpeakers.put(speakerId, speakerAbbr);
            }
        } catch (SQLException e) {
            logError(e);
        } finally {
            cleanupStatement(stmt, rs);
        }

        writeSpeakers(writeToDatabase, speakers, databaseSpeakers);
    }

    public void updateTalkContents(boolean writeToDatabase, boolean replaceTalkBodies,
            Map<String, String> talkContents, String language) {
        talkContents.forEach((talkId, contents) -> {
            logger.log(Level.INFO, () -> "Need to write talk body for " + talkId + ", length = " + contents.length());

            if (writeToDatabase || replaceTalkBodies) {
                PreparedStatement stmt = null;
                ResultSet rs = null;

                try {
                    Integer talkIdValue = talkIdsForKeys.get(talkId);

                    if (talkIdValue == null) {
                        logger.log(Level.SEVERE, () -> "Unable to write contents for talk " + talkId);
                        logger.log(Level.SEVERE, () -> "Unknown TalkID value");
                        System.exit(-1);
                    } else {
                        String talkText = StringUtils.allDecodedEntities(contents);
                        TalkBody talkBody = new TalkBody(talkText);

                        for (String table : new String[] { TABLE_TALK_BODY, TABLE_TALK_BODY2 }) {
                            stmt = conn.prepareStatement(REPLACE_INTO + tableForLanguage(table, language)
                                    + " (TalkID, Text, ProcessedText, RawText, TagVector) VALUES (?, ?, ?, ?, ?)");
                            stmt.setInt(1, talkIdValue);
                            stmt.setString(2, talkText);
                            stmt.setString(3, talkBody.getProcessedText());
                            stmt.setString(4, talkBody.getRawText());
                            stmt.setBlob(5, talkBody.getTagVectorBlob());

                            stmt.execute();
                        }
                    }
                } catch (SQLException e) {
                    logError(e);
                } finally {
                    cleanupStatement(stmt, rs);
                }
            }
        });
    }

    private void updateTalks(boolean writeToDatabase, List<String> talkIds, Map<String, String> talkHrefs,
            Map<String, Integer> talkSpeakerIds, Map<String, String> talkTitles, Map<String, int[]> pageRanges,
            Map<String, Integer> talkSequence, Map<String, Integer> talkSessionNo, Map<String, String> talkAudioUrls,
            Map<String, String[]> talkVideoUrls, String language) {

        int talkCount = 0;

        for (String talkId : talkIds) {
            int sessionId = sessionIds.get(talkSessionNo.get(talkId));
            String sessionDate = sessionDates.get(talkSessionNo.get(talkId));

            talkCount += 1;
            String[] urls = urlsForTalk(talkId, talkAudioUrls, talkVideoUrls);
            updateTalk(writeToDatabase, talkHrefs.get(talkId), talkSpeakerIds.get(talkId), talkTitles.get(talkId),
                    pageRanges.get(talkId), sessionId, sessionDate, talkSequence.get(talkId), urls, language, talkCount,
                    talkId);
        }
    }

    private void updateTalk(boolean writeToDatabase, String href, int speakerId, String title, int[] pageRange,
            int sessionId, String date, int sequence, String[] mediaUrls, String language, int talkCount,
            String talkKey) {

        PreparedStatement stmt = null;
        ResultSet rs = null;
        int talkId = existingTalkId(title, date, sequence, language);
        int maxTalkId = getMaxTalkId(tableForLanguage(TABLE_TALK, language));

        if (writeToDatabase) {
            if (talkId < 0) {
                logger.log(Level.INFO, "Inserting a talk record");
                String mediaUrl = href.replace("liahona", "general-conference").replace("/05/", "/04/").replace("/11/",
                        "/10/");

                try {
                    stmt = conn.prepareStatement(
                            INSERT_INTO + tableForLanguage(TABLE_TALK, language)
                                    + " (ID, Corpus, URL, Title, Date, SpeakerID, ListenURL, WatchURL, polymorphic_ctype_id) "
                                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
                    talkId = maxTalkId + 1;
                    stmt.setInt(1, talkId);
                    stmt.setString(2, "G");
                    stmt.setString(3, fullTalkUrl(href));
                    stmt.setString(4, title);
                    stmt.setString(5, date);
                    stmt.setInt(6, speakerId);
                    stmt.setString(7, fullTalkUrl(mediaUrl));
                    stmt.setString(8, fullTalkUrl(mediaUrl));
                    stmt.setInt(9, language.equals("spa") ? POLYMORPHIC_CTYPE_ID_ES : POLYMORPHIC_CTYPE_ID_EN);

                    stmt.execute();
                } catch (SQLException e) {
                    logError(e);
                } finally {
                    cleanupStatement(stmt, rs);
                }

                try {
                    rs = null;
                    stmt = conn.prepareStatement(REPLACE_INTO + TABLE_CONFERENCE_TALK
                            + " (TalkID, SessionID, StartPageNum, EndPageNum, Sequence, talk_ptr_id) "
                            + "VALUES (?, ?, ?, ?, ?, ?)");
                    stmt.setInt(1, talkId);
                    stmt.setInt(2, sessionId);
                    stmt.setInt(3, pageRange[0]);
                    stmt.setInt(4, pageRange[1]);
                    stmt.setInt(5, sequence);
                    stmt.setInt(6, talkId);

                    stmt.execute();
                } catch (SQLException e) {
                    logError(e);
                } finally {
                    cleanupStatement(stmt, rs);
                }

                if (mediaUrls.length == 4) {
                    try {
                        rs = null;
                        stmt = conn.prepareStatement(REPLACE_INTO + tableForLanguage(TABLE_TALK_STREAM, language)
                                + " (TalkID, AudioUrl, VideoLowUrl, VideoMedUrl, VideoHighUrl) VALUES (?, ?, ?, ?, ?)");
                        stmt.setInt(1, talkId);

                        for (int i = 0; i < mediaUrls.length; i++) {
                            stmt.setString(i + 2, mediaUrls[i]);
                        }

                        stmt.execute();
                    } catch (SQLException e) {
                        logError(e);
                    } finally {
                        cleanupStatement(stmt, rs);
                    }
                }
            } else {
                logger.log(Level.INFO, "Skip insert: it appears there is already a talk record");
            }
        } else {
            if (talkId < 0) {
                talkId = getMaxTalkId(tableForLanguage(TABLE_TALK, language)) + talkCount;
                logger.log(Level.INFO, "Need to insert talk record");
                logger.log(Level.INFO, "Need to insert talk stream record");
            } else {
                logger.log(Level.INFO, "It appears there is already a talk record");

                if (existingTalkStreamId(talkId, language) > 0) {
                    logger.log(Level.INFO, "It appears there is already a talk stream record");
                }
            }
        }

        final int id = talkId;

        talkIdsForKeys.put(talkKey, talkId);
        logger.log(Level.INFO, () -> "Talk: " + id + "; " + href + "; " + title + "; "
                + date + "; " + speakerId + "; " + sessionId + "; "
                + pageRange[0] + "-" + pageRange[1] + "; " + sequence);
    }

    private String[] urlsForTalk(String talkId, Map<String, String> talkAudioUrls,
            Map<String, String[]> talkVideoUrls) {
        String audioUrl = talkAudioUrls.get(talkId);
        String lowVideoUrl = "";
        String mediumVideoUrl = "";
        String highVideoUrl = "";
        String[] videoUrls = talkVideoUrls.get(talkId);
        boolean haveUrl = false;

        if (audioUrl != null) {
            haveUrl = true;
            audioUrl = trimMediaUrl(audioUrl);
        } else {
            audioUrl = "";
        }

        if (videoUrls != null && videoUrls.length > 0) {
            for (String url : videoUrls) {
                if (url.contains("360p")) {
                    lowVideoUrl = trimMediaUrl(url);
                    haveUrl = true;
                } else if (url.contains("720p")) {
                    mediumVideoUrl = trimMediaUrl(url);
                    haveUrl = true;
                } else if (url.contains("1080p")) {
                    highVideoUrl = trimMediaUrl(url);
                    haveUrl = true;
                }
            }
        }

        if (haveUrl) {
            return new String[] { audioUrl, lowVideoUrl, mediumVideoUrl, highVideoUrl };
        }

        return new String[] {};
    }

    private void writeCitationsToDatabase(List<Link> citationLinks) throws SQLException {
        if (citationLinks.isEmpty()) {
            return;
        }

        try (PreparedStatement stmt = conn.prepareStatement(INSERT_INTO + TABLE_CITATION
                + " (ID, TalkID, BookID, Chapter, Verses, Flag, PageColumn, MinVerse, MaxVerse) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0)");) {
            for (Link citation : citationLinks) {
                logger.log(Level.INFO, () -> "Inserting a citation record " + citation.citationId);
                logger.log(Level.INFO,
                        () -> citation.citationId + " " + talkIdsForKeys.get(citation.talkId) + " "
                                + BookFinder.sInstance.bookIdForBook(citation.book) + "/" + citation.chapter + "."
                                + citation.verses + " p. " + citation.page);

                stmt.setInt(1, citation.citationId);
                stmt.setInt(2, talkIdsForKeys.get(citation.talkId));
                stmt.setInt(3, BookFinder.sInstance.bookIdForBook(citation.book));
                stmt.setString(4, citation.chapter == null ? "0" : citation.chapter);
                stmt.setString(5, citation.verses == null ? "" : citation.verses);
                stmt.setString(6, citation.isJst ? "J" : "");
                stmt.setInt(7, citation.page);

                stmt.addBatch();
            }

            stmt.executeBatch();
        }
    }

    private void writeSpeakers(boolean writeToDatabase, Speakers speakers, Map<Integer, String> databaseSpeakers) {
        Map<Integer, JSONObject> speakersById = speakers.getSpeakersById();

        speakersById.keySet().forEach(speakerId -> {
            if (!databaseSpeakers.containsKey(speakerId)) {
                JSONObject speaker = speakersById.get(speakerId);

                if (writeToDatabase) {
                    PreparedStatement stmt = null;
                    ResultSet rs = null;

                    logger.log(Level.INFO, "Inserting a speaker record");

                    try {
                        stmt = conn.prepareStatement(INSERT_INTO + TABLE_SPEAKER
                                + " (ID, GivenNames, LastNames, Abbr, Info, NameSort) VALUES (?, ?, ?, ?, ?, ?)");
                        stmt.setInt(1, speaker.getInt(Speakers.KEY_ID));
                        stmt.setString(2, speaker.getString(Speakers.KEY_GIVENNAMES));
                        stmt.setString(3, speaker.getString(Speakers.KEY_LASTNAMES));
                        stmt.setString(4, speaker.getString(Speakers.KEY_ABBR));
                        stmt.setString(6, StringUtils.decodedEntities(speaker.getString(Speakers.KEY_LASTNAMES) + ", "
                                + speaker.getString(Speakers.KEY_GIVENNAMES)));

                        if (speaker.getString(Speakers.KEY_SUFFIX).length() <= 0) {
                            stmt.setString(5, speaker.getString(Speakers.KEY_SUFFIX));
                        }

                        stmt.execute();
                    } catch (SQLException e) {
                        logError(e);
                    } finally {
                        cleanupStatement(stmt, rs);
                    }
                } else {
                    logger.log(Level.INFO, () -> "Need to create speaker record for "
                            + speaker.getString(Speakers.KEY_LASTNAMES) + ", "
                            + speaker.getString(Speakers.KEY_GIVENNAMES));
                }
            }
        });
    }
}
