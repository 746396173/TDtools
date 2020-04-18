package com.taosdata.tools.compareTest;

import com.taosdata.tools.compareTest.utils.CommonUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataWriteToDB {
    private static final String FILE_PREFIX = "testdata";

    private String host;
    private int port;
    private String user;
    private String password;
    private String path;
    private String dbName;
    private int numOfFiles;
    private int writeClients;
    private int rowsPerRequest;
    private ExecutorService executorService;
    private String type;

    public DataWriteToDB(String host, int port, String user, String password, String path, String dbName, int numOfFiles, int writeClients, int rowsPerRequest, String type) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.path = path;
        this.dbName = dbName;
        this.numOfFiles = numOfFiles;
        this.writeClients = writeClients;
        this.rowsPerRequest = rowsPerRequest;
        this.type = type;
    }

    public void start() throws InterruptedException {
        System.out.println("Start creating databases...");

        CommonUtil.getDbOperator(host, port, user, password, dbName, type).createDb();

        System.out.println("Successfully created databases");

        System.out.println("Start loadding file and inserting data...");
        long startTime = System.currentTimeMillis();
        System.out.println("Start time: " + startTime);

        CountDownLatch countDownLatch = new CountDownLatch(writeClients);
        executorService = Executors.newFixedThreadPool(writeClients);
        for (int i = 0; i < writeClients; i++) {
            final int threadId = i;
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    Thread.currentThread().setName(String.valueOf(threadId));

                    int a = numOfFiles / writeClients;
                    int b = numOfFiles % writeClients;
                    List<String> fileList = new ArrayList<>();
                    for (int m = 0; m < a; m++) {
                        String fileName = path + "/" + FILE_PREFIX + (writeClients * m + threadId) + ".csv";
                        fileList.add(fileName);
                    }
                    if (b > 0 && b > threadId) {
                        fileList.add(path + "/" + FILE_PREFIX + (writeClients * a + threadId) + ".csv");
                    }

                    loadFile(fileList);

                    countDownLatch.countDown();
                    return;
                }
            });
        }
        countDownLatch.await();
        executorService.shutdown();

        System.out.println("Inserting completed!");
        long endTime = System.currentTimeMillis();
        System.out.println("End time: " + endTime);
        System.out.println("Total time: " + (endTime - startTime));
    }

    private void loadFile(List<String> fileList) {

        DbClient dbClient = CommonUtil.getDbOperator(host, port, user, password, dbName, type);

        Iterator<String> iterator = fileList.iterator();
        int rowNum = 0;

        while (iterator.hasNext()) {
            List<String[]> rows = new ArrayList<>();
            String lastMachineid = "";

            try {
                String filePath = iterator.next();

                BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
                String rowString;
                while ((rowString = bufferedReader.readLine()) != null) {
                    if (rowString.isEmpty() || "".equals(rowString)) {
                        continue;
                    }
                    String[] params = rowString.split(" ");
                    if (!lastMachineid.equals(params[0])) {
                        if (rowNum > 0) {
                            dbClient.writeToDb(rows);
                            rowNum = 0;
                            rows.clear();
                        }
                        lastMachineid = params[0];
                    }
                    rows.add(params);
                    rowNum++;
                    if (rowNum >= rowsPerRequest) {
                        dbClient.writeToDb(rows);
                        lastMachineid = "";
                        rowNum = 0;
                        rows.clear();
                    }
                }
                if (rowNum > 0) {
                    dbClient.writeToDb(rows);
                    rowNum = 0;
                    rows.clear();
                }
            } catch (IOException e) {
                e.printStackTrace();
                dbClient.close();
                System.exit(4);
            }
        }
        dbClient.close();
    }

    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int port = 6020;
        String user = "root";
        String password = "taosdata";
        String path = "";
        String dbName = "test";
        String type = "TDEngine";
        String sqlPath = "";
        int numOfFiles = 10;
        int writeClients = Runtime.getRuntime().availableProcessors();
        int rowsPerRequest = 0;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-h")) {
                if (i < args.length - 1) {
                    host = args[++i];
                } else {
                    System.err.println("'-h' requires a parameter, default is 127.0.0.1");
                    return;
                }
            } else if (args[i].equalsIgnoreCase("-port")) {
                if (i < args.length - 1) {
                    port = Integer.parseInt(args[++i]);
                } else {
                    System.err.println("'-port' requires a parameter, default is 6020");
                    return;
                }
            } else if (args[i].equalsIgnoreCase("-u")) {
                if (i < args.length - 1) {
                    user = args[++i];
                } else {
                    System.err.println("'-u' requires a parameter, default is root");
                    return;
                }
            } else if (args[i].equalsIgnoreCase("-p")) {
                if (i < args.length - 1) {
                    password = args[++i];
                } else {
                    System.err.println("'-p' requires a parameter, default is taosdata");
                    return;
                }
            } else if (args[i].equalsIgnoreCase("-sql")) {
                if (i < args.length - 1) {
                    sqlPath = args[++i];
                } else {
                    System.err.println("'-sql' requires a parameter");
                    return;
                }
            } else if (args[i].equalsIgnoreCase("-path")) {
                if (i < args.length - 1) {
                    path = args[++i];
                } else {
                    System.err.println("'-path' requires a parameter");
                    return;
                }
            } else if (args[i].equalsIgnoreCase("-dbname")) {
                if (i < args.length - 1) {
                    dbName = args[++i];
                } else {
                    System.err.println("'-dbname' requires a parameter, default is test.txt");
                    return;
                }
            } else if (args[i].equalsIgnoreCase("-numOfFiles")) {
                if (i < args.length - 1) {
                    numOfFiles = Integer.parseInt(args[++i]);
                } else {
                    System.err.println("'-numOfFiles' requires a parameter");
                    return;
                }
            } else if (args[i].equalsIgnoreCase("-writeClients")) {
                if (i < args.length - 1) {
                    writeClients = Integer.parseInt(args[++i]);
                } else {
                    System.err.println("'-writeClients' requires a parameter, default is " + writeClients);
                    return;
                }
            } else if (args[i].equalsIgnoreCase("-rowsPerRequest")) {
                if (i < args.length - 1) {
                    rowsPerRequest = Integer.parseInt(args[++i]);
                } else {
                    System.err.println("'-rowsPerRequest' requires a parameter");
                    return;
                }
            } else if (args[i].equalsIgnoreCase("-type")) {
                if (i < args.length - 1) {
                    type = args[++i];
                    if ("influxDb".equalsIgnoreCase(type) && port == 6020) {
                        port = 8086;
                    }
                } else {
                    System.err.println("'-type' requires a parameter, default is TDEngine");
                    return;
                }
            }
        }
        if (CommonUtil.isEmpty(path)) {
            System.err.println("'-path' is required");
            return;
        }
        if (0 >= numOfFiles) {
            System.err.println("'-numOfFiles' is required");
            return;
        }
        if (0 >= writeClients) {
            System.err.println("'-writeClients' is required");
            return;
        }
        if (0 >= rowsPerRequest) {
            System.err.println("'-rowsPerRequest' is required");
            return;
        }
        System.out.println("parameters");
        System.out.printf("----host:%s\n", host);
        System.out.printf("----port:%d\n", port);
        System.out.printf("----user:%s\n", user);
        System.out.printf("----path:%s\n", path);
        System.out.printf("----dbName:%s\n", dbName);

        if (!CommonUtil.isEmpty(sqlPath)) {
            DataReadFromDB dataReadFromDB = new DataReadFromDB(host, port, user, password, dbName, type);
            dataReadFromDB.readData(sqlPath);
            return;
        }

        System.out.printf("----numOfFiles:%d\n", numOfFiles);
        System.out.printf("----writeClients:%d\n", writeClients);
        System.out.printf("----rowsPerRequest:%d\n", rowsPerRequest);
        DataWriteToDB dataWriteToDB = new DataWriteToDB(host, port, user, password, path, dbName, numOfFiles, writeClients, rowsPerRequest, type);
        dataWriteToDB.start();
    }
}
