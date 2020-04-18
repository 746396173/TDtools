package com.taosdata.tools.collections;

import com.alibaba.fastjson.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TDEngineConsumerTest {

    @Test
    void consume() throws IOException {
        String filePath = "/media/psf/Home/Taosdata/weichai-kafka/src/main/resources/test.txt";
        BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
        List<JSONObject> list = new ArrayList<>();

        String line = "";
        while (null != (line = bufferedReader.readLine())) {
            JSONObject jsonObject = JSONObject.parseObject(line);
            list.add(jsonObject);
        }
        TDEngineConsumer tdEngineConsumer = new TDEngineConsumer("localhost", 0, "root", "taosdata", "test");
        tdEngineConsumer.consume(list);
    }
}