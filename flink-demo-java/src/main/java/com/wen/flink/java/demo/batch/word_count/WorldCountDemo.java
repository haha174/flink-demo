package com.wen.flink.java.demo.batch.word_count;



import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class WorldCountDemo {
    public static void main(String[] args) {
        String input="C:\\Users\\wenqchen\\Desktop\\data\\test.txt";
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> lines=env.readTextFile(input);
        try {
            lines.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                @Override
                public void flatMap(String values, Collector< Tuple2<String,Integer>> out) throws Exception {
                    String[] valueArray=values.split(" ");
                    for (String value :valueArray ){
                        out.collect(new Tuple2<>(value,1));
                    }
                }
            }).groupBy(0).sum(1).print();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
