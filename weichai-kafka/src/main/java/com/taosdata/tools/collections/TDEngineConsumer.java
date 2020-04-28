package com.taosdata.tools.collections;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tools.WeichaiType;
import com.taosdata.tools.bean.CarBean;
import com.taosdata.tools.bean.WeichaiBean;
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
    //    private static AtomicLong insertSuccessRows = new AtomicLong(0);
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

    public void consume(List<JSONObject> set) {
        if (set.size() == 0) {
            log.error("set's size is 0");
            return;
        }
        Iterator<JSONObject> iterator = set.iterator();
        //List<CarWorkBean> listWork = new ArrayList<>();
        Field[] fields = CarBean.class.getDeclaredFields();
        while (iterator.hasNext()) {
            JSONObject jsonObject = iterator.next();
            if (!jsonObject.containsKey("terminalID") || !jsonObject.containsKey("dataTime")) {
                iterator.remove();
                log.error(jsonObject.toJSONString() + "is not valid.");
            } else {
                long dataTime = jsonObject.getLong("dataTime");
                String terminalId = jsonObject.getString("terminalID");

                for (Field field : fields) {
                    if (field.getAnnotation(WeichaiType.class) != null) {
                        String keyName = field.getAnnotation(WeichaiType.class).value();
                        String filedName = field.getName();
                        Class cls = field.getType();

                        JSONObject jsonObject1 = jsonObject.getJSONObject(filedName);
                        WeichaiBean obj = (WeichaiBean) JSONObject.parseObject(jsonObject1.toJSONString(), cls);
                        try {
                            Method setTerminalID = cls.getMethod("setTerminalID", String.class);
                            Method setDataTime = cls.getMethod("setDataTime", long.class);
                            setTerminalID.invoke(obj, terminalId);
                            setDataTime.invoke(obj, dataTime);
                            if ("locationInfo".equals(filedName)) {
                                JSONObject locationInfo = jsonObject1.getJSONObject("address");
                                if (null == locationInfo) {
                                    continue;
                                }
                                Method setDistrictCode = cls.getMethod("setDistrictCode", String.class);
                                setDistrictCode.invoke(obj, locationInfo.getString("districtCode"));
//                                Method setDirection = cls.getMethod("setDirection", String.class);
//                                Method setLonOffset = cls.getMethod("setLonOffset", String.class);
                                Method setProvince = cls.getMethod("setProvince", String.class);
                                setProvince.invoke(obj, locationInfo.getString("province"));

                                Method setCity = cls.getMethod("setCity", String.class);
                                setCity.invoke(obj, locationInfo.getString("city"));

                                Method setCityCode = cls.getMethod("setCityCode", String.class);
                                setCityCode.invoke(obj, locationInfo.getString("cityCode"));

                                Method setProvinceCode = cls.getMethod("setProvinceCode", String.class);
                                setProvinceCode.invoke(obj, locationInfo.getString("provinceCode"));

                                Method setDistrict = cls.getMethod("setDistrict", String.class);
                                setDistrict.invoke(obj, locationInfo.getString("district"));

                            }
                        } catch (NoSuchMethodException e) {
                            e.printStackTrace();
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        } catch (InvocationTargetException e) {
                            e.printStackTrace();
                        }
                        if (map.containsKey(keyName)) {
                            map.get(keyName).add(obj);
                        } else {
                            map.put(keyName, new ArrayList<WeichaiBean>() {{
                                add(obj);
                            }});
                        }
                    }
                }
                //2020-04-20 add
                //JSONObject workObject = jsonObject.getJSONObject("work");
                //long dataTime = jsonObject.getLong("dataTime");
                //String terminalId = jsonObject.getString("terminalID");
                //workObject.put("dataTime", dataTime);
                //workObject.put("terminalID", terminalId);

                //CarWorkBean workBean = (CarWorkBean) JSONObject.parseObject(workObject.toJSONString(), CarWorkBean.class);
                //listWork.add(workBean);
            }
        }

        for (Map.Entry<String, List<WeichaiBean>> entry : map.entrySet()) {
            String beanName = entry.getKey();
            if (entry.getValue().size() >= 50) {
                List<WeichaiBean> listWork = entry.getValue();

                msgNum.getAndAdd(listWork.size());
                Collections.sort(listWork, new Comparator<WeichaiBean>() {
                    @Override
                    public int compare(WeichaiBean workBean, WeichaiBean t1) {
                        return (int) (workBean.getDataTime() - t1.getDataTime());
                    }
                });

                insertRows.getAndAdd(listWork.size());
                int affectRows = 0;
                int time = writeToDb(beanName, listWork);
//        insertSuccessRows.getAndAdd(affectRows);
                totalTime.getAndAdd(time);
                listWork.clear();
//        return affectRows;
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
        String filePath = "/media/psf/Home/Taosdata/TDtools/weichai-kafka/src/main/resources/test.txt";
        BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
        List<JSONObject> list = new ArrayList<>();

        String line = "";
        int i = 0;
        while (null != (line = bufferedReader.readLine()) && i < 100) {
            i++;
            JSONObject jsonObject = JSONObject.parseObject(line);
            list.add(jsonObject);
        }
        TDEngineConsumer tdEngineConsumer = new TDEngineConsumer("localhost", 0, "root", "taosdata", "test");
        tdEngineConsumer.consume(list);
        bufferedReader.close();
    }
}
