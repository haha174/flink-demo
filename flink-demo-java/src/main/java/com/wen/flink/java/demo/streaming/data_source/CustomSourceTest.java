package com.wen.flink.java.demo.streaming.data_source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.stream.Stream;

public class CustomSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
       // DataStreamSource<Long> dataStream=env.addSource(new CustomNonParallelSourceFunction()).setParallelism(1);
         DataStreamSource<Long> dataStream=env.addSource(new CustomParallelSourceFunction()).setParallelism(2);


        dataStream.print();
        env.execute("CustomSourceTest");
    }
}
