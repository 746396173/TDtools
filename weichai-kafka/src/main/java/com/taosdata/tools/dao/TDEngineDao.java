package com.taosdata.tools.dao;

import com.taosdata.tools.bean.CarWorkBean;

import java.sql.*;
import java.util.List;

public class TDEngineDao {
    private String host;
    private int port;
    private String user;
    private String password;
    private Connection connection;
    private String dbName;

    private final static String METRICS_NAME = "work";

    private final static String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";

    static {
        try {
            Class.forName(TSDB_DRIVER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public TDEngineDao(String host, int port, String user, String password, String dbName) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.connection = getConnection();
        this.dbName = dbName;
    }

    private Connection getConnection() {
        Connection connection = null;
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

    public void createDbAndSuperTable(boolean dropDb) {
        String sql = "";
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            if (dropDb) {
                stmt.executeUpdate("drop database if exists " + dbName);
            }

            sql = "create database if not exists " + dbName;
            stmt.executeUpdate(sql);
            System.out.printf("Successfully executed: %s\n", sql);

            sql = "use " + dbName;
            stmt.executeUpdate(sql);
            System.out.printf("Successfully executed: %s\n", sql);

            sql = "create table if not exists " + METRICS_NAME + " (ts timestamp, flueGasTemp int, retarderTorque0F int, " +
                    "batteryVoltage float, engineOilPress int, coolWaterTemp int, airTemp int, superchargerPress int, " +
                    "manifoldTemp int, inAirPress int, inVoltageECM float, airPress float, engineOilTemp float) " +
                    "tags(devid BIGINT)";
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

    public int writeToDb(List<CarWorkBean> list) {
        int affectRows = 0;

        String sql = String.format("insert into dev%s using " + METRICS_NAME + " tags(%s) values ", list.get(0).getTerminalID(), list.get(0).getTerminalID());
        for (int i = 0; i < list.size(); i++) {
            CarWorkBean workBean = list.get(i);
            sql += String.format(" (%d,%d,%d,%f,%d,%d,%d,%d,%d,%d,%f,%f,%f)", workBean.getDataTime(), workBean.getFuelTemp(), workBean.getRetarderTorque0F(), workBean.getBatteryVoltage(), workBean.getEngineOilPress(), workBean.getCoolWaterTemp(), workBean.getAirTemp(), workBean.getSuperchargerPress(), workBean.getManifoldTemp(), workBean.getInAirPress(), workBean.getInVoltageECM(), workBean.getAirPress(), workBean.getEngineOilTemp());
        }
        try {
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("use " + dbName);
            affectRows = stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(4);
        }

        return affectRows;
    }

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
