package com.taosdata.tools.kafka.consumer;

import com.taosdata.tools.dao.TDEngineDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class WeichaiConsumer {
    private static final Logger log = LogManager.getLogger(WeichaiConsumer.class);
    private static String jdbcUrl = "jdbc:TAOS://%s:%s/log?user=%s&password=%s";

    private static String acturl = null;
    private static String configFile;
    private static String topic;
    private static String host = "127.0.0.1";
    private static int port = 0;
    private static String user = "root";
    private static String password = "taosdata";
    private static String dbName = "test";
    private static int insertThread = 2;
    private static int insertBatch = 100;
    private static boolean dropDb = false;
    private static boolean statistic = true;
    private static long delay = 2000L;
    private static long interval = 1000L;

    public static void parseArg(String[] args) {
        String usgae = "usage: java -jar weichaiConsumer.jar \n-topic topic \n-configFile configFile \n-host host \n-port port \n-user user \n-password password \n-db db \n-insertThread insertNum \n-insertBatch batchSize\n-writeMode insert\n-dropDb false\n-statistic false\n-delay 2000\n-interval 1000\n";
        for (int i = 0; i < args.length; i++) {
            if ("-h".equalsIgnoreCase(args[i])) {
                System.out.println(usgae);
                System.exit(1);
            }
            if ("-topic".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                topic = args[++i];
            } else if ("-configFile".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                configFile = args[++i];
            } else if ("-host".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                host = args[++i];
            } else if ("-port".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                port = Integer.parseInt(args[++i]);
            } else if ("-user".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                user = args[++i];
            } else if ("-password".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                password = args[++i];
            } else if ("-db".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                dbName = args[++i];
            } else if ("-insertThread".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                insertThread = Integer.parseInt(args[++i]);
            } else if ("-insertBatch".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                insertBatch = Integer.parseInt(args[++i]);
            } else if ("-dropDb".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                dropDb = Boolean.parseBoolean(args[++i]);
            } else if ("-statistic".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                statistic = Boolean.parseBoolean(args[++i]);
            } else if ("-delay".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                delay = Long.parseLong(args[++i]);
            } else if ("-interval".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                interval = Long.parseLong(args[++i]);
            }
        }
    }

    private static void startTimer(List<ConfigConsumer> consumerList) {
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        final List<ConfigConsumer> list = consumerList;
        service.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                long msgNum = 0l, insertRows = 0l, insertSuccessRows = 0l;
                for (int i = 0; i < list.size(); i++) {
                    StatisticParam statisticParam = list.get(i).getStatisticParam();

                    msgNum += statisticParam.getMsgNum();
                    insertRows += statisticParam.getInsertRows();
                    insertSuccessRows += statisticParam.getInsertSuccessRows();
                }
                System.out.println(String.format("msgNum: %d, insertRows: %d, insertSuccessRows: %d", msgNum, insertRows, insertSuccessRows));
                log.info(String.format("msgNum: %d, insertRows: %d, insertSuccessRows: %d", msgNum, insertRows, insertSuccessRows));
            }
        }, delay, interval, TimeUnit.MILLISECONDS);
    }

    public static void main(String[] args) throws Exception {
        parseArg(args);
        TDEngineDao tdEngineDao = new TDEngineDao(host, port, user, password, dbName);
        tdEngineDao.createDbAndSuperTable(dropDb);

        InputStream inputStream = new FileInputStream(configFile);
        Properties props = new Properties();
        props.load(inputStream);

        ExecutorService executorService = Executors.newFixedThreadPool(insertThread);
        List<ConfigConsumer> consumerList = new ArrayList<>(insertThread);
        ConfigConsumer configConsumer = null;
        for (int i = 0; i < insertThread; i++) {
            ConsumerParam consumerParam = new ConsumerParam(topic, props, insertBatch);
            configConsumer = new ConfigConsumer(consumerParam, tdEngineDao);
            consumerList.add(configConsumer);
            executorService.execute((Runnable) configConsumer);
        }

        if (statistic) {
            startTimer(consumerList);
        }

        executorService.shutdown();
    }
}

