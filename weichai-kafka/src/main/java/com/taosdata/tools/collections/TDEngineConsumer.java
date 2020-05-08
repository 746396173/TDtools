package com.taosdata.tools.collections;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tools.bean.WeichaiBean;
import com.taosdata.tools.WeichaiType;
import com.taosdata.tools.bean.CarBean;
import com.taosdata.tools.bean.LocationInfoBean;
import com.taosdata.tools.dao.TDEngineDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TDEngineConsumer {
    private static final Logger log = LogManager.getLogger(TDEngineConsumer.class);
    private static long delay = 2000L;
    private static long interval = 1000L;

    private static AtomicLong msgNum = new AtomicLong(0);
    private static AtomicLong insertRows = new AtomicLong(0);
    private static AtomicLong totalTime = new AtomicLong(0);
    HashMap<String, List<WeichaiBean>> map = new HashMap<>();

    static {
        Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                //System.out.println("OK");
                System.out.println(String.format("msgNum: %d, insertRows: %d, totalTime: %d", msgNum.get(), insertRows.get(), totalTime.get()));
                log.info(String.format("msgNum: %d, insertRows: %d, totalTime: %d", msgNum.get(), insertRows.get(), totalTime.get()));
            }
        }, delay, interval, TimeUnit.MILLISECONDS);
    }

    // db host
    private String host = "10.3.52.125";
    // db port
    private int port = 0;
    // db user
    private String user = "root";
    // db pass
    private String password = "taosdata";
    // db Name
    private String dbName = "test";

    private TDEngineDao tdEngineDao;

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
        Field[] fields = CarBean.class.getDeclaredFields();
        while (iterator.hasNext()) {
            JSONObject jsonObject = iterator.next();
            if (!jsonObject.containsKey("terminalID") || !jsonObject.containsKey("dataTime") || !jsonObject.containsKey("work")) {
                iterator.remove();
                log.error(jsonObject.toJSONString() + "is valid or don't contain 'terminalID' or 'dataTime'.");
            } else {
                long dataTime = jsonObject.getLong("dataTime");
                String terminalId = jsonObject.getString("terminalID");

                for (Field field : fields) {
                    if (field.getAnnotation(WeichaiType.class) != null) {
                        String keyName = field.getAnnotation(WeichaiType.class).value();
                        //System.out.println(field.getName());
                        String filedName = field.getName();
                        Class cls = field.getType();
                        JSONObject jsonObject1 = jsonObject.getJSONObject(filedName);
                        WeichaiBean obj = (WeichaiBean) JSONObject.parseObject(jsonObject1.toJSONString(), cls);
                        if (keyName.equals("LocationInfoBean") && obj instanceof LocationInfoBean) {
                            LocationInfoBean infoBean = (LocationInfoBean) obj;
                            if (null != infoBean.getAddress()) {
                                if ((null != infoBean.getAddress().get("province"))) {
                                    infoBean.setProvince(infoBean.getAddress().get("province"));
                                }
                                if (null != infoBean.getAddress().get("city")) {
                                    infoBean.setCity(infoBean.getAddress().get("city"));
                                }
                                if (null != infoBean.getAddress().get("cityCode")) {
                                    infoBean.setCityCode(infoBean.getAddress().get("cityCode"));
                                }
                                if (null != infoBean.getAddress().get("provinceCode")) {
                                    infoBean.setProvinceCode(infoBean.getAddress().get("provinceCode"));
                                }
                                if (null != infoBean.getAddress().get("provinceCode")) {
                                    infoBean.setProvinceCode(infoBean.getAddress().get("provinceCode"));
                                }
                                infoBean.setDistrict(null != infoBean.getAddress().get("district") ? infoBean.getAddress().get("district") : null);
                            }
                        }
                        try {
                            Method setTerminalID = cls.getMethod("setTerminalID", String.class);
                            Method setDataTime = cls.getMethod("setDataTime", long.class);
                            setTerminalID.invoke(obj, terminalId);
                            setDataTime.invoke(obj, dataTime);
                        } catch (NoSuchMethodException e) {
                            e.printStackTrace();
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        } catch (InvocationTargetException e) {
                            e.printStackTrace();
                        }
//                        keyName = keyName + "_" + terminalId;
                        if (map.containsKey(keyName)) {
                            map.get(keyName).add(obj);
                        } else {
                            map.put(keyName, new ArrayList<WeichaiBean>() {{
                                add(obj);
                            }});
                        }
                    }
                }
            }
        }

        for (Map.Entry<String, List<WeichaiBean>> entry : map.entrySet()) {
            String beanName = entry.getKey();
            if (entry.getValue().size() >= 100) {
                List<WeichaiBean> listWork = entry.getValue();

                msgNum.getAndAdd(listWork.size());
//                Collections.sort(listWork, new Comparator<WeichaiBean>() {
//                    @Override
//                    public int compare(WeichaiBean workBean, WeichaiBean t1) {
//                        return (int) (workBean.getDataTime() - t1.getDataTime());
//                    }
//                });

                int time = writeToDb(beanName, listWork);
                totalTime.getAndAdd(time);
                insertRows.getAndAdd(listWork.size());
                listWork.clear();
            }
        }
    }

    private int writeToDb(String beanName, List<WeichaiBean> list) {
        try {
            return tdEngineDao.writeToDb(beanName, list);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(4);
        }
        return 0;
    }

    public static void main(String[] args) throws IOException {
        String filePath = "/Users/chang/Taosdata/TDtools/weichai-kafka/src/main/resources/test.txt";
        BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
        List<JSONObject> list = new ArrayList<>();

        String line = "";
        while (null != (line = bufferedReader.readLine())) {
            JSONObject jsonObject = JSONObject.parseObject(line);
            list.add(jsonObject);
        }
        TDEngineConsumer tdEngineConsumer = new TDEngineConsumer("10.211.55.3", 0, "root", "taosdata", "test");
        tdEngineConsumer.consume(list);
    }
}
