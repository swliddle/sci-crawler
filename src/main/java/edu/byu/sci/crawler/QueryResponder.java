package edu.byu.sci.crawler;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This interface is used as a callback mechanism for database queries. Each
 * time {@code DatabaseConnector} processes a particular type of query, it
 * invokes the {@code processResult} method for each row in the
 * {@code ResultSet}.
 */
public interface QueryResponder {
    /**
     * Process the current result in the ResultSet.
     * 
     * @param rs Active ResultSet to process.
     * @throws SQLException
     */
    public void processResult(ResultSet rs) throws SQLException;
}
