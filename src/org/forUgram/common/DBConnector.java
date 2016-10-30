package org.forUgram.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;

public final class DBConnector {

    
    static class DBAccess {
        
        static final String HOSTNAME = "127.0.0.1";
        static final String SCHEMA = "fetch_data";
        static final String USERNAME = "root";
        static final String PASSWORD = "@dnpqrpdla";
        
        static String getURL() {
            return "jdbc:mysql://" + HOSTNAME + ":3306/" + SCHEMA
                                                                          + "?useUnicode=true"
                                                                          + "&characterEncoding=UTF8"
                                                                          + "&jdbcCompliantTruncation=false"
                                                                          + "&useOldUTF8Behavior=true";
        }
    }
    
    private static final String DRIVER = "com.mysql.jdbc.Driver"; 
    private static final ThreadLocal<Connection> connect = new ThreadLocalConnection();

    private DBConnector() {
    }

    public static Connection connect() {
        return connect.get();
    }

    private final static class ThreadLocalConnection extends ThreadLocal<Connection> {

        private static final Collection<Connection> connect = new LinkedList();

        @Override
        protected final Connection initialValue() {
            try {
                Class.forName(DRIVER);
                Connection c = DriverManager.getConnection(DBAccess.getURL(), DBAccess.USERNAME, DBAccess.PASSWORD);
                ThreadLocalConnection.connect.add(c);
                return c;
            }
            catch (ClassNotFoundException | SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
