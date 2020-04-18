package com.taosdata.tools.compareTest;

import java.sql.*;
import java.util.List;

public class TDEngineDbClientImpl implements DbClient {
    private static final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
    private static final String METRICS_NAME = "devices";

    private String host = "127.0.0.1";
    private int port = 0;
    private String user = "root";
    private String password = "taosdata";
    private String dbName = "test.txt";

    private Connection connection;

    static {
        try {
            Class.forName(TSDB_DRIVER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public TDEngineDbClientImpl() {
    }

    public TDEngineDbClientImpl(String host, int port, String user, String password, String dbName) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.dbName = dbName;
    }


    private Connection getConnection() {
        try {
            if (null == connection || connection.isClosed()) {
                String jdbcUrl = String.format("jdbc:TAOS://%s:%d/?user=%s&password=%s", host, port, user, password);
                connection = DriverManager.getConnection(jdbcUrl);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(4);
        }

        return connection;
    }

    @Override
    public void createDb() {
        String sql = "";
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            sql = "create database if not exists " + dbName;
            stmt.executeUpdate(sql);
            System.out.printf("Successfully executed: %s\n", sql);

            sql = "use " + dbName;
            stmt.executeUpdate(sql);
            System.out.printf("Successfully executed: %s\n", sql);

            sql = "create table if not exists " + METRICS_NAME + " (ts timestamp, temperature int, humidity int) " +
                    "tags(devid int, devname binary(16), devgroup int)";
            stmt.executeUpdate(sql);
            System.out.printf("Successfully executed: %s\n", sql);

        } catch (SQLException e) {
            e.printStackTrace();
            System.out.printf("Failed to execute SQL: %s\n", sql);
            System.exit(4);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(4);
        }
    }

    @Override
    public int writeToDb(List<String[]> list) {
        String sql = String.format("insert into dev%s using devices tags(%s,'%s',%s) values ", list.get(0)[0], list.get(0)[0], list.get(0)[1], list.get(0)[2]);
        for (int i = 0; i < list.size(); i++) {
            sql += String.format(" (%s,%s,%s)", list.get(i)[3], list.get(i)[4], list.get(i)[5]);
        }
        int affectRows = 0;
        System.out.println("start:" + System.currentTimeMillis());
        try {
            connection = getConnection();
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("use " + dbName);
            affectRows = stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(4);
        }
        System.out.println("end:" + System.currentTimeMillis());
        return affectRows;
    }

    @Override
    public Object executeSql(String sql) {
        connection = getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("use " + dbName);
            ResultSet result = statement.executeQuery(sql);
            return result;
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(4);
        }
        return null;
    }

    @Override
    public void close() {
        if (null != connection) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(4);
            }
        }
    }
}
