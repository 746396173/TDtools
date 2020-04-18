package com.taosdata.tools.compareTest;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class InfluxDbClientImpl implements DbClient {
    private String host = "127.0.0.1";
    private int port = 8086;
    private String user = "root";
    private String password = "taosdata";
    private String dbName = "test.txt";

    private InfluxDB influxDB = null;

    public InfluxDbClientImpl() {
    }

    public InfluxDbClientImpl(String host, int port, String user, String password, String dbName) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.dbName = dbName;
    }

    private InfluxDB getConnection() {
        if (null == influxDB || !influxDB.isBatchEnabled()) {
            String serverURL = "http://" + host + ":" + port;
            influxDB = InfluxDBFactory.connect(serverURL, user, password);
        }
        return influxDB;
    }

    @Override
    public void createDb() {
        InfluxDB influxDB = getConnection();
        influxDB.createDatabase(dbName);
        close();
    }

    @Override
    public int writeToDb(List<String[]> list) {
        InfluxDB influxDB = getConnection();
        try {
            BatchPoints batchPoints = BatchPoints
                    .database(dbName)
                    .build();
            for (int i = 0; i < list.size(); i++) {
                String[] row = list.get(i);
                batchPoints.point(Point.measurement("devices")
                        .time(Long.parseLong(row[3]), TimeUnit.MILLISECONDS)
                        .tag("devid", row[0])
                        .tag("devname", row[1])
                        .tag("devgroup", row[2])
                        .addField("time", Long.parseLong(row[3]))
                        .addField("temperature", Integer.parseInt(row[4]))
                        .addField("humidity", Integer.parseInt(row[5]))
                        .build());
            }
            influxDB.write(batchPoints);
        } catch (RuntimeException e) {
            e.printStackTrace();
            close();
            return 0;
        }
        return list.size();
    }

    @Override
    public Object executeSql(String sql) {
        InfluxDB influxDB = getConnection();
        return influxDB.query(new Query(sql, dbName));
    }

    @Override
    public void close() {
        if (null != influxDB) {
            influxDB.close();
            influxDB = null;
        }
    }
}
