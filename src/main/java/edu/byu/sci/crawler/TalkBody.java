package edu.byu.sci.crawler;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.rowset.serial.SerialBlob;

public class TalkBody {
    private final Logger logger = Logger.getLogger(TalkBody.class.getName());
    private final Database database;

    public TalkBody(Database database) {
        this.database = database;
    }

    private void executePreparedStringBlobString(String sql, String stringParam, Blob blobParam, String stringParam2) {
        PreparedStatement stmt = null;

        logger.log(Level.INFO, () -> "executePrepared " + sql);
        try {
            stmt = database.getConnection().prepareStatement(sql);
            stmt.setString(1, stringParam);
            stmt.setBlob(2, blobParam);
            stmt.setString(3, stringParam2);
            stmt.executeUpdate();
        } catch (SQLException e) {
            database.logError(e);
        } finally {
            database.cleanupStatement(stmt, null);
        }
    }

    public void processTalksToRawStringsAndTagVectors(final String table) {
        String sql = "SELECT TalkID, ProcessedText FROM " + table + " WHERE RawText IS NULL";
        logger.log(Level.INFO, () -> "Process query: " + sql);

        database.query(sql, new QueryResponder() {
            @Override
            public void processResult(ResultSet rs) throws SQLException {
                String talkId = rs.getString(1);
                String processedTalk = rs.getString(2);

                logger.log(Level.INFO, () -> "Updating talk " + talkId);

                if (processedTalk == null) {
                    logger.log(Level.SEVERE, () -> "Processed talk is null for ID " + talkId);
                    return;
                }

                byte[] tagVector = tagVectorForHtmlString(processedTalk);
                Blob tagBlob = new SerialBlob(tagVector);
                String cleanString = SearchTerm.cleanHtmlString(processedTalk).toString();
                if (cleanString.length() != processedTalk.length() || tagVector.length != cleanString.length()) {
                    logger.log(Level.WARNING, () -> "Have length issue " + talkId + " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                    int processedLen = processedTalk.length();
                    int cleanLen = cleanString.length();
                    int vectorLen = tagVector.length;
                    int maxLen = Math.min(cleanLen, processedLen);
                    maxLen = Math.min(maxLen, vectorLen);

                    for (int i = 0; i < maxLen; i++) {
                        final int index = i;
                        char pc = processedTalk.charAt(i);
                        char cc = cleanString.charAt(i);

                        if (tagVector[i] == 0) {
                            if (cc == ' ') {
                                if (Character.isAlphabetic(pc) || Character.isDigit(pc)) {
                                    logger.log(Level.WARNING, () -> "Suspicious character " + index + " [" + pc + "] -> ["
                                            + cc + "] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
                                } else {
                                    logger.log(Level.INFO,
                                            () -> "Subsuming character " + index + " [" + pc + "] -> [" + cc + "]");
                                }
                            } else {
                                if (Character.toLowerCase(pc) == cc) {
                                    logger.log(Level.INFO, () -> "Character " + index + " [" + pc + "] -> [" + cc + "]");
                                } else {
                                    logger.log(Level.WARNING, () -> "Suspicious character " + index + " [" + pc + "] -> ["
                                            + cc + "] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
                                }
                            }
                        } else if (tagVector[i] == 1) {
                            logger.log(Level.INFO, () -> "Tag character " + index + " [" + pc + "] -> [" + cc + "]");
                        } else {
                            logger.log(Level.WARNING, () -> "Unexpected tagVector value (" + tagVector[index]
                                    + ") <*><*><*><*><*><*><*><*><*><*><*><*><*><*><*>");
                        }
                    }
                }

                executePreparedStringBlobString(
                        "UPDATE " + table + " SET RawText = ?, TagVector = ? WHERE TalkID = ?", cleanString, tagBlob,
                        talkId);
            }
        });
    }

    private byte[] tagVectorForHtmlString(String string) {
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
}
