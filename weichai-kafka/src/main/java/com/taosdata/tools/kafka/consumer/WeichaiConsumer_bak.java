package com.taosdata.tools.kafka.consumer;

import com.alibaba.fastjson.JSON;
import com.taosdata.tools.dao.TDEngineDao;
import com.taosdata.tools.bean.CarBean;
import com.taosdata.tools.bean.CarWorkBean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.*;

public class WeichaiConsumer_bak {
    private static final Logger log = LogManager.getLogger(WeichaiConsumer_bak.class);
    private static String jdbcUrl = "jdbc:TAOS://%s:%s/log?user=%s&password=%s";

    private static String acturl = null;
    private static String configFile;
    private static String topic;
    private static String host = "127.0.0.1";
    private static int port = 0;
    private static String user = "root";
    private static String password = "taosdata";
    private static String dbName = "test";
    private static int insertThread = 1;
    private static int insertBatch = 100;
    private static String writeMode = "insert";
    private static DecimalFormat decimalFormat = new DecimalFormat("#.00");
    private static long startTime;
    private static boolean dropDb = false;
    private static boolean statistic = false;
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
            } else if ("-writeMode".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                writeMode = args[++i];
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

    public static void main(String[] args) throws IOException {
        TDEngineDao tdEngineDao = new TDEngineDao(host, port, user, password, dbName);
        int updateLimit = 1000;

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "tackiest");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("taosdata"));
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        Map<String, List<CarWorkBean>> devMap = new HashMap<>();

        int i = 0;
        System.out.println("start time: " + System.currentTimeMillis());
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
            if (records.count() > 0) {
                System.out.println(records.count());
            }
            for (ConsumerRecord<String, String> record : records) {
                CarBean carBean = JSON.parseObject(record.value(), CarBean.class);

                carBean.getWork().setDataTime(carBean.getDataTime());
                carBean.getWork().setTerminalID(carBean.getTerminalID());

                if (null != devMap.get(carBean.getTerminalID())) {
                    devMap.get(carBean.getTerminalID()).add(carBean.getWork());
                    i++;
                } else {
                    devMap.put(carBean.getTerminalID(), new ArrayList<CarWorkBean>() {{
                        add(carBean.getWork());
                    }});
                    i++;
                }
                if (i >= updateLimit) {
                    //TODO 修改代码
//                    tdEngineDao.writeToDb(devMap);
                    devMap = new HashMap<>();
                }
            }
            if (records.isEmpty() && null != devMap) {
                //TODO 修改代码
//                tdEngineDao.writeToDb(devMap);
                devMap = new HashMap<>();
                System.out.println("end time: " + System.currentTimeMillis());
            }
        }
    }
}

