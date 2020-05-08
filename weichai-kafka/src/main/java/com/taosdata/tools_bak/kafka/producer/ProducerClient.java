package com.taosdata.tools_bak.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class ProducerClient {
    private static final Logger log = LogManager.getLogger(ProducerClient.class);

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private static String topic;

    private static long num;

    private static String configFile;

    public static void parseArg(String[] args) {
        String usgae = "usage: java -jar weichai-kafka-0.0.1-SNAPSHOT.jar " +
                "-topic topic " +
                "-configFile configFile" +
                "-num num";
        for (int i = 0; i < args.length; i++) {
            if ("-h".equalsIgnoreCase(args[i])) {
                System.out.println(usgae);
                System.exit(1);
            }
            if ("-topic".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                topic = args[++i];
            } else if ("-num".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                num = Integer.parseInt(args[++i]);
            } else if ("-configFile".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                configFile = args[++i];
            }
        }
    }

    public static class ValueGen {
        int center;
        int max;
        Random rand;

        public ValueGen(int center, int max) {
            this.center = center;
            this.max = max;

            this.rand = new Random();
        }

        double next() {
            double v = this.rand.nextGaussian();
            if (v < 0) {
                v = 1;
            }

            if (v > 10) {
                v = 10;
            }

            return (this.max / 10.00) * v;
        }
    }

    public static void main(String[] args) throws IOException, ParseException {
        parseArg(args);

        InputStream inputStream = new FileInputStream(configFile);
        Properties properties = new Properties();
        properties.load(inputStream);

        List<Map<String, String>> resultList = new ArrayList<>();

        log.info("read {} rows.", Integer.valueOf(resultList.size()));
        KafkaProducer<String, String> producer = new KafkaProducer(properties);

        String carInfo = "{\"locationInfo\":{\"acc\":1,\"altitude\":149,\"address\":{\"districtCode\":\"360731\",\"province\":\"江西省\",\"city\":\"赣州市\",\"cityCode\":\"360700\",\"provinceCode\":\"3f60000\",\"district\":\"于都县\"},\"lonType\":0,\"geoHash\":\"ws9grq\",\"latitude\":25.923546,\"latType\":1,\"gpsTime\":1586235765000,\"terminalID\":\"1440005897255\",\"latOffset\":25.920479,\"speed\":57.3,\"lonOffset\":115.280112,\"direction\":180,\"longitude\":115.275133,\"status\":0},\"dataTime\":%d,\"work\":%s,\"terminalID\":\"%d\",\"state1\":{\"dataTime\":1586235765000,\"torqueMode\":\"加速踏板/操作者选择\",\"cruiseEnable\":\"未使能\"}}";

        long dataStartTime = System.currentTimeMillis();
        DecimalFormat df1 = new DecimalFormat(".0");
        DecimalFormat df4 = new DecimalFormat(".0000");
        ValueGen batteryVoltageGen = new ValueGen(50, 100);
        ValueGen inVoltageECMGen = new ValueGen(10, 50);
        ValueGen airPressGen = new ValueGen(50, 100);
        ValueGen engineOilTempGen = new ValueGen(50, 100);
        long sendRows = 0L;
        long start = System.currentTimeMillis();
        Random random = new Random();

        for (long i = 0; i < num; i++) {
            long time = dataStartTime + i;
            String workStr = "{\"flueGasTemp\":%d,\"retarderTorque0F\":%d,\"batteryVoltage\":%f,\"engineOilPress\":%d,\"coolWaterTemp\":%d,\"airTemp\":%d,\"superchargerPress\":%d,\"manifoldTemp\":%d,\"dataTime\":%d,\"inAirPress\":%d,\"inVoltageECM\":%f,\"airPress\":%f,\"engineOilTemp\":%f}";
            String workRecord = String.format(workStr,
                    random.nextInt(10),
                    random.nextInt(10),
                    Double.parseDouble(df1.format(batteryVoltageGen.next())),
                    random.nextInt(1000),
                    random.nextInt(100),
                    random.nextInt(40),
                    random.nextInt(50),
                    random.nextInt(90),
                    time,
                    random.nextInt(200),
                    Double.parseDouble(df1.format(inVoltageECMGen.next())),
                    Double.parseDouble(df1.format(airPressGen.next())),
                    Double.parseDouble(df4.format(engineOilTempGen.next())));
            String record = String.format(carInfo, time, workRecord, random.nextInt(1000));
            ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, record);
            producer.send(msg);
            sendRows++;
        }
        //列出topic的相关信息
        List<PartitionInfo> partitions = producer.partitionsFor(topic);
        for (PartitionInfo p : partitions) {
            System.out.println(p);
        }

        System.out.println("send message over.");
        log.info("finished insert {} rows, used {} ms.", Long.valueOf(sendRows), Long.valueOf(System.currentTimeMillis() - start));
        producer.flush();
        producer.close(Duration.ofMillis(100));
    }
}
