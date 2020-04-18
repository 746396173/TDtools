package com.taosdata.tools.compareTest;

import java.util.List;

public interface DbClient {
    public void createDb();

    public int writeToDb(List<String[]> list);

    public void close();

    public Object executeSql(String sql);
}
