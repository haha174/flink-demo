package com.wen.flink.java.demo.batch.data_source;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class DataSourceApi {
    /**
     *  从集合中获取data set
     * @param env
     * @throws Exception
     */
     void fromCollection(ExecutionEnvironment env) throws Exception {
        List<Integer> data=new ArrayList<>();
        for (int i=0;i<10;i++){
            data.add(i);
        }
        env.fromCollection(data).print();
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment=ExecutionEnvironment.getExecutionEnvironment();
        new DataSourceApi().fromCollection(executionEnvironment);
    }
}
