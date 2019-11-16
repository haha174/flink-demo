package com.wen.flink.java.demo.batch.counter;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

public class CounterApp {
    static ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

    void myCounter() throws Exception {
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(i);
        }
        DataSet<Integer> dataSet1 = executionEnvironment.fromCollection(data);
        DataSet<Integer> info=dataSet1.map(new RichMapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return null;
            }

            @Override
            public void open(Configuration config) {

            }
        }).setParallelism(1);
        info.print();
       //JobExecutionResult jobExecutionResult= executionEnvironment.execute("myCounter");
    }

    public static void main(String[] args) throws Exception {
        new CounterApp().myCounter();
    }
}
