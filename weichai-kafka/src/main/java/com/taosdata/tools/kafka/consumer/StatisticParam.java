package com.taosdata.tools.kafka.consumer;

public class StatisticParam {
    private long msgNum;

    private long insertRows;

    private long insertSuccessRows;

    public void setMsgNum(long msgNum) {
        this.msgNum = msgNum;
    }

    public void setInsertRows(long insertRows) {
        this.insertRows = insertRows;
    }

    public void setInsertSuccessRows(long insertSuccessRows) {
        this.insertSuccessRows = insertSuccessRows;
    }

    public String toString() {
        return "StatisticParam(msgNum=" + getMsgNum() + ", insertRows=" + getInsertRows() + ", insertSuccessRows=" + getInsertSuccessRows() + ")";
    }

    public long getMsgNum() {
        return this.msgNum;
    }

    public long getInsertRows() {
        return this.insertRows;
    }

    public long getInsertSuccessRows() {
        return this.insertSuccessRows;
    }
}
