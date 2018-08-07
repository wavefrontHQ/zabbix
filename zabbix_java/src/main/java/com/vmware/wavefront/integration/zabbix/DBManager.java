package com.vmware.wavefront.integration.zabbix;
import java.sql.*;

/**
 * Database Manager responsible for acquiring and returning
 * db connection from mysql database.
 */
public class DBManager {

    public static DBManager manager;
    static {
        manager = new DBManager();
    }

    public static DBManager getInstance() {
        return manager;
    }

    /**
     * if connection is not found, returns exception
     * @return
     */
    public Connection getConnection() throws Exception {

        String host = ZabbixConfig.getProperty("mysql.db.host");
        String port = ZabbixConfig.getProperty("mysql.db.port");
        String dbname = ZabbixConfig.getProperty("mysql.db.name");
        String userid = ZabbixConfig.getProperty("mysql.db.user");
        String pw = ZabbixConfig.getProperty("mysql.db.password");
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            String jdbcurl = "jdbc:MySQL://" + host + ":" + port + "/" + dbname;
            Connection conn = DriverManager.getConnection(jdbcurl, userid, pw);
            return conn;
        } catch(SQLException e) {
            throw e;
        }
    }
}
