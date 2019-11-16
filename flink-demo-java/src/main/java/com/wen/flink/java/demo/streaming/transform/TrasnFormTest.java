package com.wen.flink.java.demo.streaming.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class TrasnFormTest {
    public static void main(String[] args) throws Exception {
        splitFunction();
    }

    public static void splitFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> dataStream = env.addSource(new CustomNonParallelSourceFunction()).setParallelism(1);
        SplitStream<Long> resultStream= dataStream.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                List<String> output=new ArrayList<>();
                if (value%2==0){
                    output.add("even");
                }else {
                    output.add("odd");
                }
                return output;
            }
        });
        resultStream.print();
        env.execute("splitFunction");
    }
    public void unionFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> dataStream = env.addSource(new CustomNonParallelSourceFunction()).setParallelism(1);
        SingleOutputStreamOperator<Long> resultStream =dataStream.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value ;
            }
        }).filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                if (value >0) {
                    return true;
                }else {
                    return false;
                }
            }
        });
        dataStream.union(resultStream).print();
        env.execute("unionFunction");
    }
}
