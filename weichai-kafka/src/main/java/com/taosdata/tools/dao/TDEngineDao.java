package com.taosdata.tools.dao;

import com.taosdata.tools.bean.WeichaiBean;
import com.taosdata.tools.WeichaiTypeName;
import com.taosdata.tools.bean.LocationInfoBean;
import com.taosdata.tools.bean.WorkBean;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

            //Reflections reflections = new Reflections(new ConfigurationBuilder()
            // .forPackages("com.taosdata.tools.bean").addScanners(new FieldAnnotationsScanner()));
            Set<Class<? extends WeichaiBean>> subTypes = new HashSet<>();
            subTypes.add(WorkBean.class);
            subTypes.add(LocationInfoBean.class);

            for (Class cls : subTypes) {
                if (null == cls.getAnnotation(WeichaiTypeName.class)) {
                    continue;
                }
                String tableName = ((WeichaiTypeName) cls.getAnnotation(WeichaiTypeName.class)).value();
                StringBuilder tableSql = new StringBuilder("create table if not exists ")
                        .append(tableName).append(" (ts timestamp, ");
                Field[] filelds = cls.getDeclaredFields();
                for (Field field : filelds) {
                    String typeName = field.getType().getName();
                    System.out.println(typeName);
                    if ("int".equals(typeName)) {
                        tableSql.append(field.getName()).append(" ").append("int, ");
                    }
                    if ("double".equals(typeName)) {
                        tableSql.append(field.getName()).append(" ").append("float, ");
                    }
                    if ("java.lang.String".equals(typeName)) {
                        tableSql.append(field.getName()).append(" ").append("BINARY(200), ");
                    }
                }
                sql = tableSql.toString();
                if (filelds.length > 0) {
                    sql = tableSql.toString().substring(0, tableSql.length() - 2);
                }
                System.out.println(sql);
                sql += ") tags(devid BINARY(50))";
                stmt.executeUpdate(sql);
                System.out.printf("Successfully executed: %s\n", sql);
            }


        } catch (SQLException e) {
            e.printStackTrace();
            System.out.printf("Failed to execute SQL: %s\n", sql);
            System.exit(4);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(4);
        }
    }

    public int writeToDb(String beanName, List<WeichaiBean> list) throws ClassNotFoundException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException {
        Class cls = Class.forName("com.taosdata.tools.bean." + beanName);
        //List<WeichaiBean> list = new ArrayList<>();
        if (null == cls.getAnnotation(WeichaiTypeName.class)) {
            return 0;
        }

        String tableName = ((WeichaiTypeName) cls.getAnnotation(WeichaiTypeName.class)).value();

        try {
            String sql = "import into ";
            int totalTime = 0;
            Statement stmt = connection.createStatement();

            for (int i = 0; i < list.size(); i++) {
                WeichaiBean weichaiBean = list.get(i);
                String sqlParam = "dev_" + tableName + "_" + weichaiBean.getTerminalID() + " using " + tableName + " tags('" + weichaiBean.getTerminalID() + "') values (";
                Field[] filelds = cls.getDeclaredFields();
                sqlParam += weichaiBean.getDataTime() + ",";

                for (Field field : filelds) {
                    if (field.getType().equals(Map.class)) {
                        continue;
                    }
                    String typeName = field.getType().getName();
                    String methodN = field.getName();
                    String methodName = "";
                    if (methodN.length() > 1) {
                        char chr = methodN.charAt(1);
                        if (Character.isUpperCase(chr)) {
                            methodName = "get" + methodN;
                        }
                    }
                    if (methodName.length() < 3) {
                        methodName = "get" + methodN.substring(0, 1).toUpperCase() + methodN.substring(1);
                    }
                    Method method = cls.getMethod(methodName, null);
                    if ("java.lang.String".equals(typeName)) {
                        String s = (String) method.invoke(weichaiBean);
                        sqlParam += "'" + s + "',";
                    } else if ("int".equals(typeName)) {
                        int s = (int) method.invoke(weichaiBean);
                        sqlParam += s + ",";
                    }
                    if ("double".equals(typeName)) {
                        double s = (double) method.invoke(weichaiBean);
                        sqlParam += s + ",";
                    }
                    if ("long".equals(typeName)) {
                        long s = (long) method.invoke(weichaiBean);
                        sqlParam += s + ",";
                    }
                }

                if ((sql.length() + (sqlParam.substring(0, sqlParam.length() - 1) + ")").length()) > 10000) {
                    i--;
                    sql = sql.substring(0, sql.length() - 1);
                    stmt.executeUpdate("use " + dbName);
                    System.out.println(sql);
                    long s = System.currentTimeMillis();
                    stmt.executeUpdate(sql);
                    long t = System.currentTimeMillis();
                    totalTime += t - s;
                    sql = "import into ";
                } else {
                    sql += sqlParam.substring(0, sqlParam.length() - 1) + ") ";
                }
            }

            if (sql.length() > 20) {
                stmt.executeUpdate("use " + dbName);
                System.out.println(sql);
                long s = System.currentTimeMillis();
                stmt.executeUpdate(sql);
                long t = System.currentTimeMillis();
                totalTime += t - s;
            }
            return totalTime;
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(4);
        }
        return 0;

        //int affectRows = 0;
        //String sql = String.format("import into dev%s using " + METRICS_NAME + " tags('%s') values ", list.get(0).getTerminalID(), list.get(0).getTerminalID());
        //for (int i = 0; i < list.size(); i++) {
        //    CarWorkBean workBean = list.get(i);
        //    sql += String.format(" (%d,%d,%d,%f,%d,%d,%d,%d,%d,%d,%f,%f,%f)", workBean.getDataTime(), workBean.getFuelTemp(), workBean.getRetarderTorque0F(), workBean.getBatteryVoltage(), workBean.getEngineOilPress(), workBean.getCoolWaterTemp(), workBean.getAirTemp(), workBean.getSuperchargerPress(), workBean.getManifoldTemp(), workBean.getInAirPress(), workBean.getInVoltageECM(), workBean.getAirPress(), workBean.getEngineOilTemp());
        //}
        //try {
        //    Statement stmt = connection.createStatement();
        //    stmt.executeUpdate("use " + dbName);
        //   //System.out.println(sql);
        //    long s = System.currentTimeMillis();
        //    affectRows = stmt.executeUpdate(sql);
        //    long t = System.currentTimeMillis();
        //    return (int)(t-s);
        //} catch (SQLException e) {
        //    e.printStackTrace();
        //    System.exit(4);
        //}
        //return 0;

        //return affectRows;
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
