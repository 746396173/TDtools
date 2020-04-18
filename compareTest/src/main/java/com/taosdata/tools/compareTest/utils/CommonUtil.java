package com.taosdata.tools.compareTest.utils;

import com.taosdata.tools.compareTest.DbClient;
import com.taosdata.tools.compareTest.InfluxDbClientImpl;
import com.taosdata.tools.compareTest.TDEngineDbClientImpl;

public class CommonUtil {
    public static boolean isEmpty(CharSequence string) {
        return (string == null) || (string.length() == 0);
    }


    public static DbClient getDbOperator(String host, int port, String user, String password, String dbName, String type) {
        DbClient dbClient = null;
        switch (type) {
            case "TDEngine":
                dbClient = new TDEngineDbClientImpl(host, port, user, password, dbName);
                break;
            case "influxDb":
                dbClient = new InfluxDbClientImpl(host, port, user, password, dbName);
        }
        return dbClient;
    }
}
