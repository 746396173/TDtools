package com.taosdata.tools.compareTest;

import com.taosdata.tools.compareTest.utils.CommonUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class DataReadFromDB {
    private String host;
    private int port;
    private String user;
    private String password;
    private String dbName;
    private String type;

    public DataReadFromDB(String host, int port, String user, String password, String dbName, String type) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.dbName = dbName;
        this.type = type;
    }

    public void readData(String path) {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(path))) {
            String sql;
            while ((sql = bufferedReader.readLine()) != null) {
                if (!CommonUtil.isEmpty(sql)) {
                    executeSql(sql);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(4);
        }
    }

    private void executeSql(String sql) {
        System.out.println("execute sql: " + sql);
        long startTime = System.currentTimeMillis();
        System.out.println("start time: " + startTime);

        DbClient dbClient = CommonUtil.getDbOperator(host, port, user, password, dbName, type);
        dbClient.executeSql(sql);
        dbClient.close();

        long endTime = System.currentTimeMillis();
        System.out.println("end time: " + endTime);
        System.out.println("total time: " + (endTime - startTime));

    }
}
