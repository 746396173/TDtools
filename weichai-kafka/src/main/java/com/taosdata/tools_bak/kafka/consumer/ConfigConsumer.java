package com.taosdata.tools_bak.kafka.consumer;

import com.alibaba.fastjson.JSON;
import com.taosdata.tools_bak.dao.TDEngineDao;
import com.taosdata.tools_bak.bean.CarBean;
import com.taosdata.tools_bak.bean.CarWorkBean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.*;

public class ConfigConsumer implements Runnable {
    private static final Logger log = LogManager.getLogger(ConfigConsumer.class);

    private KafkaConsumer<String, String> consumer;

    private long insertSqlRows;

    private long successRows;

    private long msgNum;

    private TDEngineDao tdEngineDao;

    public ConfigConsumer(ConsumerParam consumerParam, TDEngineDao tdEngineDao) throws Exception {
        this.consumer = new KafkaConsumer(consumerParam.getProperties());
        this.consumer.subscribe(Arrays.asList(new String[]{consumerParam.getTopic()}));
        this.tdEngineDao = tdEngineDao;
    }

    public void run() {
        doBatchInsert();
    }

    private void doBatchInsert() {
        int updateLimit = 1000;

        Map<String, List<CarWorkBean>> devMap = new HashMap<>();

        int i = 0;
        int retry = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));

            if (records.isEmpty()) {
                if (retry >= 1000) {
                    if (devMap.size() > 0) {
                        insertSqlRows += records.count();
                        for (Map.Entry<String, List<CarWorkBean>> entry : devMap.entrySet()) {
                            List<CarWorkBean> list = entry.getValue();

                            int successRows = writeToDb(list);
                            this.successRows += successRows;
                        }

                        i = 0;
                        devMap = new HashMap<>();
                        retry = 0;
                    } else {
                        System.out.println("end");
                        return;
                    }
                } else {
                    retry++;
                }
            } else {
                this.msgNum += records.count();

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
                        insertSqlRows += i;
                        for (Map.Entry<String, List<CarWorkBean>> entry : devMap.entrySet()) {
                            List<CarWorkBean> list = entry.getValue();

                            int successRows = writeToDb(list);
                            this.successRows += successRows;
                        }

                        devMap = new HashMap<>();
                        retry = 0;
                        i = 0;
                    }
                }
            }
        }
    }

    private int writeToDb(List<CarWorkBean> list) {
        Collections.sort(list, new Comparator<CarWorkBean>() {
            @Override
            public int compare(CarWorkBean carBean, CarWorkBean t1) {
                return (int) (carBean.getDataTime() - t1.getDataTime());
            }
        });
        return tdEngineDao.writeToDb(list);
    }

    public StatisticParam getStatisticParam() {
        StatisticParam statisticParam = new StatisticParam();
        statisticParam.setMsgNum(this.msgNum);
        statisticParam.setInsertRows(this.insertSqlRows);
        statisticParam.setInsertSuccessRows(this.successRows);
        return statisticParam;
    }

}
