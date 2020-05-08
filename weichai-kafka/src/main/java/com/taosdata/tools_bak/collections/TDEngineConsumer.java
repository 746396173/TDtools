package com.taosdata.tools_bak.collections;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tools_bak.bean.CarWorkBean;
import com.taosdata.tools_bak.dao.TDEngineDao;
import com.taosdata.tools_bak.kafka.producer.ProducerClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TDEngineConsumer {
    private static final Logger log = LogManager.getLogger(ProducerClient.class);
    private static long delay = 2000L;
    private static long interval = 1000L;

    private static AtomicLong msgNum = new AtomicLong(0);
    private static AtomicLong insertRows = new AtomicLong(0);
//    private static AtomicLong insertSuccessRows = new AtomicLong(0);
    private static AtomicLong totalTime = new AtomicLong(0);

    static {
        Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                System.out.println(String.format("msgNum: %d, insertRows: %d, insertSuccessRows: %d, totalTime: %d, avgTime: %f", msgNum.get(), insertRows.get(), totalTime.get()));
                log.info(String.format("msgNum: %d, insertRows: %d, insertSuccessRows: %d, totalTime: %d, avgTime: %f", msgNum.get(), insertRows.get(), totalTime.get()));
            }
        }, delay, interval, TimeUnit.MILLISECONDS);
    }

    // db host
    private String host = "localhost";
    // db port
    private int port = 0;
    // db user
    private String user = "root";
    // db pass
    private String password = "taosdata";
    // db Name
    private String dbName = "test";

    private TDEngineDao tdEngineDao;

    private List<CarWorkBean> listWork = new ArrayList<>();

    public TDEngineConsumer() {
        this.tdEngineDao = new TDEngineDao(host, port, user, password, dbName);
        this.tdEngineDao.createDbAndSuperTable(false);
    }

    public TDEngineConsumer(String host, int port, String user, String password, String dbName) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.dbName = dbName;
        this.tdEngineDao = new TDEngineDao(host, port, user, password, dbName);
        this.tdEngineDao.createDbAndSuperTable(false);
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void consume(List<JSONObject> list) {
        if (list.size() == 0) {
            log.error("list's size is 0");
            return;
        }
        Iterator<JSONObject> iterator = list.iterator();
        while (iterator.hasNext()) {
            JSONObject jsonObject = iterator.next();
            if (!jsonObject.containsKey("terminalID") || !jsonObject.containsKey("dataTime") || !jsonObject.containsKey("work")) {
                iterator.remove();
                log.error(jsonObject.toJSONString() + "is valid or don't contain 'work'.");
            } else {
                JSONObject workObject = jsonObject.getJSONObject("work");
                long dataTime = jsonObject.getLong("dataTime");
                String terminalId = jsonObject.getString("terminalID");
                workObject.put("dataTime", dataTime);
                workObject.put("terminalID", terminalId);

                CarWorkBean workBean = (CarWorkBean) JSONObject.parseObject(workObject.toJSONString(), CarWorkBean.class);
                listWork.add(workBean);
            }
        }
        if (listWork.size() >= 200) {
            msgNum.addAndGet(listWork.size());

            Collections.sort(listWork, new Comparator<CarWorkBean>() {
                @Override
                public int compare(CarWorkBean workBean, CarWorkBean t1) {
                    return (int) (workBean.getDataTime() - t1.getDataTime());
                }
            });

            insertRows.getAndAdd(list.size());
            long s = System.currentTimeMillis();
            int affectRows = writeToDb(listWork);
            long t = System.currentTimeMillis();
//            insertSuccessRows.getAndAdd(affectRows);
            listWork.clear();
            totalTime.getAndAdd(t - s);
        }
    }

    private int writeToDb(List<CarWorkBean> list) {
        return tdEngineDao.writeToDb(list);
    }

    public static void main(String[] args) throws IOException {
//        String filePath = "/Users/chang/Taosdata/TDtools/weichai-kafka/src/main/resources/test.txt";
//        BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
//        List<JSONObject> list = new ArrayList<>();
//
//        String line = "";
//        while (null != (line = bufferedReader.readLine())) {
//            JSONObject jsonObject = JSONObject.parseObject(line);
//            list.add(jsonObject);
//        }
//        TDEngineConsumer tdEngineConsumer = new TDEngineConsumer("10.211.55.3", 0, "root", "taosdata", "test");
//        tdEngineConsumer.consume(list);
    }

}
