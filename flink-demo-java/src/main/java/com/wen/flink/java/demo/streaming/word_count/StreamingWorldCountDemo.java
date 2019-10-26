package com.wen.flink.java.demo.streaming.word_count;


import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class StreamingWorldCountDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> source = env.socketTextStream("10.148.185.205", 9999);
        source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String values, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] valueArray = values.split(" ");
                for (String value : valueArray) {
                    out.collect(new Tuple2<>(value, 1));
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print().setParallelism(1);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
