package com.wen.flink.java.demo.streaming.time;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WindowProcessTimeApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource=env.socketTextStream("localhost",9999);
        dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String,Integer>> out) throws Exception {
                if (value!=null && value.length()>0){
                    String[] valueArray=value.split("-");
                    for (String str :valueArray){
                        out.collect(new Tuple2<String,Integer>(str,1));
                    }
                }
            }
        }).keyBy(0).timeWindow(Time.days(5)).sum(1).print();
        env.execute("WindowApp");
    }
}
