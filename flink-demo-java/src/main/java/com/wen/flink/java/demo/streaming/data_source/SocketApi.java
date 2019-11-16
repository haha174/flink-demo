package com.wen.flink.java.demo.streaming.data_source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.scala.DataStream;

public class SocketApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream=env.socketTextStream("10.169.154.212",9999);
        dataStream.print();
        env.execute("SocketApi");

    }
}
