package com.taosdata.tools.kafka.consumer;

import java.util.Properties;

public class ConsumerParam {
    private String topic;
    private Properties properties;
    private int batchSize;

    public ConsumerParam(String topic, Properties properties, int batchSize) {
        this.topic = topic;
        this.properties = properties;
        this.batchSize = batchSize;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getTopic() {
        return this.topic;
    }

    public Properties getProperties() {
        return this.properties;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public String toString() {
        return "ConsumerParam(topic=" + getTopic() + ", properties=" + getProperties() + ", batchSize=" + getBatchSize() + ")";
    }
}
