package edu.byu.sci.crawler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Database {
    private Connection conn = null;
    private Logger logger = Logger.getLogger(Database.class.getName());
    
    Database() {
        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost/sci2p?" + "user=sci2puser&password=sci44access");
        } catch (SQLException e) {
            logger.log(Level.SEVERE, () -> "SQLException: " + e.getMessage());
            logger.log(Level.SEVERE, () -> "SQLState: " + e.getSQLState());
            logger.log(Level.SEVERE, () -> "VendorError: " + e.getErrorCode());
        }
    }

    public int getMaxCitationId() {
        Statement stmt = null;
        ResultSet rs = null;
        int maxId = -1;

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT MAX(ID) FROM citation");
            rs.next();

            maxId = rs.getInt(1);
        } catch (SQLException e) {
            logger.log(Level.SEVERE, () -> "SQLException: " + e.getMessage());
            logger.log(Level.SEVERE, () -> "SQLState: " + e.getSQLState());
            logger.log(Level.SEVERE, () -> "VendorError: " + e.getErrorCode());
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) {
                    // ignore
                }

                rs = null;
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) {
                    // ignore
                }

                stmt = null;
            }
        }

        return maxId;
    }
}
