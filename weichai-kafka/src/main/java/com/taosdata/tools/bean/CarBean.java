package com.taosdata.tools.bean;

import java.util.Map;

public class CarBean {
    private String terminalID;
    private long dataTime;
    // 位置信息
    private Map<String, Object> locationInfo;
    // 工况信息
    private CarWorkBean work;
    // 状态信息1
    private Map<String, Object> state1;
    // 状态信息2
    private Map<String, Object> state2;
    // 状态信息3
    private Map<String, Object> state3;
    private Map<String, Object> consumption;
    // 动力信息
    private Map<String, Object> power;
    // 车速信息
    private Map<String, Object> speed;
    // 累计信息
    private Map<String, Object> total;

    public String getTerminalID() {
        return terminalID;
    }

    public void setTerminalID(String terminalID) {
        this.terminalID = terminalID;
    }

    public long getDataTime() {
        return dataTime;
    }

    public void setDataTime(long dataTime) {
        this.dataTime = dataTime;
    }

    public Map<String, Object> getLocationInfo() {
        return locationInfo;
    }

    public void setLocationInfo(Map<String, Object> locationInfo) {
        this.locationInfo = locationInfo;
    }

    public CarWorkBean getWork() {
        return work;
    }

    public void setWork(CarWorkBean work) {
        this.work = work;
    }

    public Map<String, Object> getState1() {
        return state1;
    }

    public void setState1(Map<String, Object> state1) {
        this.state1 = state1;
    }

    public Map<String, Object> getState2() {
        return state2;
    }

    public void setState2(Map<String, Object> state2) {
        this.state2 = state2;
    }

    public Map<String, Object> getState3() {
        return state3;
    }

    public void setState3(Map<String, Object> state3) {
        this.state3 = state3;
    }

    public Map<String, Object> getConsumption() {
        return consumption;
    }

    public void setConsumption(Map<String, Object> consumption) {
        this.consumption = consumption;
    }

    public Map<String, Object> getPower() {
        return power;
    }

    public void setPower(Map<String, Object> power) {
        this.power = power;
    }

    public Map<String, Object> getSpeed() {
        return speed;
    }

    public void setSpeed(Map<String, Object> speed) {
        this.speed = speed;
    }

    public Map<String, Object> getTotal() {
        return total;
    }

    public void setTotal(Map<String, Object> total) {
        this.total = total;
    }
}
