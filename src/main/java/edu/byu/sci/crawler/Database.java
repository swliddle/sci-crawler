package edu.byu.sci.crawler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Database {
    private class DatabaseException extends RuntimeException {
        public DatabaseException(String description) {
            super(description);
        }
    }

    private static final Map<String, String> sConferenceDescription = new HashMap<>();
    private static final Map<String, String> sConferenceAbbreviation = new HashMap<>();
    private static final Map<String, String> sSessionAbbreviation = new HashMap<>();

    private static final int POLYMORPHIC_CTYPE_ID_EN = 31;
    private static final int POLYMORPHIC_CTYPE_ID_ES = 62;

    private static final String[] SESSION_TITLES_EN = { "priesthood", "women&rsquo;s", "sunday morning",
            "sunday afternoon", "saturday morning", "saturday afternoon", "saturday evening" };

    private static final String[] SESSION_TITLES_ES = { "del sacerdocio", "general de mujeres",
            "del domingo por la mañana", "del domingo por la tarde", "del sábado por la mañana",
            "del sábado por la tarde", "del sábado por la noche" };

    private static final String TABLE_CITATION = "citation";
    private static final String TABLE_CONFERENCE = "conference";
    private static final String TABLE_CONFERENCE_SESSION = "conf_session";
    private static final String TABLE_CONFERENCE_TALK = "conference_talk";
    private static final String TABLE_TALK = "talk";

    private Connection conn = null;
    private Logger logger = Logger.getLogger(Database.class.getName());
    private Map<Integer, Integer> sessionIds = new HashMap<>();
    private Map<Integer, String> sessionDates = new HashMap<>();
    
    Database() {
        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost/sci2p?" + "user=sci2puser&password=sci44access");
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

    private void cleanupStatement(Statement stmt, ResultSet rs) {
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

    private String descriptionForSession(String session, GregorianCalendar date, String language) {
        DateFormat dateFormat = new SimpleDateFormat("d MMM yyyy",
                language.equals("spa") ? new Locale("es") : Locale.US);

        return session + ", " + dateFormat.format(date.getTime());
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

    private void logError(SQLException e) {
        logger.log(Level.SEVERE, () -> "SQLException: " + e.getMessage());
        logger.log(Level.SEVERE, () -> "SQLState: " + e.getSQLState());
        logger.log(Level.SEVERE, () -> "VendorError: " + e.getErrorCode());
    }    

    private String tableForLanguage(String table, String language) {
        return language.equals("spa") ? table + "_es" : table;
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
                            "INSERT INTO " + tableForLanguage(TABLE_CONFERENCE, language)
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
            Map<String, Integer> talkSessionNo) {
 
        int conferenceId = updateConference(writeToDatabase, conferenceDescription(year, language, annual),
                conferenceAbbreviation(year, language, annual), year, annual, issueDate, language);
        updateSessions(writeToDatabase, sessions, language, saturdayDate, sundayDate, conferenceId);
        updateTalks(writeToDatabase, talkIds, talkHrefs, talkSpeakerIds, talkTitles, pageRanges, talkSequence,
                talkSessionNo, language);
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
                            "INSERT INTO " + tableForLanguage(TABLE_CONFERENCE_SESSION, language)
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

    private void updateTalks(boolean writeToDatabase, List<String> talkIds, Map<String, String> talkHrefs,
            Map<String, Integer> talkSpeakerIds, Map<String, String> talkTitles, Map<String, int[]> pageRanges,
            Map<String, Integer> talkSequence, Map<String, Integer> talkSessionNo, String language) {

        int talkCount = 0;

        for (String talkId : talkIds) {
            int sessionId = sessionIds.get(talkSessionNo.get(talkId));
            String sessionDate = sessionDates.get(talkSessionNo.get(talkId));

            talkCount += 1;
            updateTalk(writeToDatabase, talkHrefs.get(talkId), talkSpeakerIds.get(talkId), talkTitles.get(talkId),
                    pageRanges.get(talkId), sessionId, sessionDate, talkSequence.get(talkId), language, talkCount);
        }
    }

    private void updateTalk(boolean writeToDatabase, String href, int speakerId, String title,
            int[] pageRange, int sessionId, String date, int sequence, String language, int talkCount) {

        PreparedStatement stmt = null;
        ResultSet rs = null;
        int talkId = existingTalkId(title, date, sequence, language);

        if (writeToDatabase) {
            if (talkId < 0) {
                logger.log(Level.INFO, "Inserting a talk record");
                String mediaUrl = href.replace("liahona", "general-conference").replace("/05/", "/04/").replace("/11/", "/10/");

                try {
                    stmt = conn.prepareStatement(
                            "INSERT INTO " + tableForLanguage(TABLE_TALK, language)
                                    + " (Corpus, TalkURL, Title, Date, SpeakerID, ListenURL, WatchURL, polymorphic_ctype_id) "
                                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            Statement.RETURN_GENERATED_KEYS);
                    stmt.setString(1, "G");
                    stmt.setString(2, href);
                    stmt.setString(3, title);
                    stmt.setString(4, date);
                    stmt.setInt(5, speakerId);
                    stmt.setString(6, mediaUrl);
                    stmt.setString(7, mediaUrl);
                    stmt.setInt(8, language.equals("spa") ? POLYMORPHIC_CTYPE_ID_ES : POLYMORPHIC_CTYPE_ID_EN);

                    stmt.execute();
                    rs = stmt.getGeneratedKeys();

                    if (rs.next()) {
                        talkId = rs.getInt(1);
                    } else {
                        throw new DatabaseException("Unable to retrieve generated talk ID");
                    }
                } catch (SQLException e) {
                    logError(e);
                } finally {
                    cleanupStatement(stmt, rs);
                }

                try {
                    rs = null;
                    stmt = conn.prepareStatement("REPLACE INTO " + TABLE_CONFERENCE_TALK
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
            } else {
                logger.log(Level.INFO, "Skip insert: it appears there is already a talk record");
            }
        } else {
            if (talkId < 0) {
                talkId = getMaxTalkId(tableForLanguage(TABLE_TALK, language)) + talkCount;
                logger.log(Level.INFO, "Need to insert talk record");
            } else {
                logger.log(Level.INFO, "It appears there is already a talk record");
            }
        }

        final int id = talkId;

        logger.log(Level.INFO, () -> "Talk: " + id + "; " + href + "; " + title + "; "
                + date + "; " + speakerId + "; " + sessionId + "; "
                + pageRange[0] + "-" + pageRange[1] + "; " + sequence);
    }
}
