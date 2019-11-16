package com.wen.flink.java.demo.batch.transform;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TransFormAPI {


    void myMapFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(i);
        }
        DataSet<Integer> dataSet = env.fromCollection(data);
        dataSet.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value + 1;
            }
        }).print();
    }

    void myFilterFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(i);
        }
        DataSet<Integer> dataSet = env.fromCollection(data);
        dataSet.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                if (value < 5) {
                    return false;
                } else {
                    return true;
                }
            }
        }).print();
    }


    void myMapPartitionFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(i);
        }
        DataSet<Integer> dataSet = env.fromCollection(data).setParallelism(6);


        dataSet.mapPartition(new MapPartitionFunction<Integer, Integer>() {
            @Override
            public void mapPartition(Iterable<Integer> values, Collector<Integer> out) throws Exception {
                Iterator<Integer> ite = values.iterator();
                while (ite.hasNext()) {
                    out.collect((ite.next() + 1));
                }
            }
        }).print();
    }


    void myFirstFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(i);
        }
        DataSet<Integer> dataSet = env.fromCollection(data);
        dataSet.first(2).print();
    }


    void myFlatMapFunction(ExecutionEnvironment env) throws Exception {
        List<String> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add("student_" + i);
        }
        DataSet<String> dataSet = env.fromCollection(data);
        dataSet.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] array = value.split("_");
                for (int i = 0; i < array.length; i++) {
                    out.collect(array[i]);
                }
            }
        }).print();
    }


    void myDistinctFunction(ExecutionEnvironment env) throws Exception {
        List<String> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add("student_" + i);
        }
        DataSet<String> dataSet = env.fromCollection(data);
        dataSet.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] array = value.split("_");
                for (int i = 0; i < array.length; i++) {
                    out.collect(array[i]);
                }
            }
        }).distinct().print();
    }


    void myJoinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<String, Integer>> data1 = new ArrayList<>();
        List<Tuple2<String, Integer>> data2 = new ArrayList<>();
        data1.add(new Tuple2<>("h", 1));
        data1.add(new Tuple2<>("c", 2));
        data1.add(new Tuple2<>("a", 3));


        data2.add(new Tuple2<>("h", 11));
        data2.add(new Tuple2<>("c", 22));
        data2.add(new Tuple2<>("o", 44));
        DataSet<Tuple2<String, Integer>> dataSet1 = env.fromCollection(data1);
        DataSet<Tuple2<String, Integer>> dataSet2 = env.fromCollection(data2);

        dataSet1.join(dataSet2).where(0).equalTo(0).with(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
                return new Tuple3<>(first.f0, first.f1, second.f1);
            }
        }).print();
    }


    void myOutJoinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<String, Integer>> data1 = new ArrayList<>();
        List<Tuple2<String, Integer>> data2 = new ArrayList<>();
        data1.add(new Tuple2<>("h", 1));
        data1.add(new Tuple2<>("c", 2));
        data1.add(new Tuple2<>("a", 3));


        data2.add(new Tuple2<>("h", 11));
        data2.add(new Tuple2<>("c", 22));
        data2.add(new Tuple2<>("o", 44));
        DataSet<Tuple2<String, Integer>> dataSet1 = env.fromCollection(data1);
        DataSet<Tuple2<String, Integer>> dataSet2 = env.fromCollection(data2);

        dataSet1.leftOuterJoin(dataSet2).where(0).equalTo(0).with(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
                if (second != null) {
                    return new Tuple3<>(first.f0, first.f1, second.f1);
                } else {
                    return new Tuple3<>(first.f0, first.f1, 0);
                }
            }
        }).print();


        dataSet1.rightOuterJoin(dataSet2).where(0).equalTo(0).with(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
                if (first != null) {
                    return new Tuple3<>(first.f0, first.f1, second.f1);
                } else {
                    return new Tuple3<>("default first", 0, second.f1);
                }
            }
        }).print();

        dataSet1.fullOuterJoin(dataSet2).where(0).equalTo(0).with(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
                if (second != null && first!=null) {
                    return new Tuple3<>(first.f0, first.f1, second.f1);
                } else if(second==null){
                    return new Tuple3<>(first.f0, first.f1, 0);
                }  else {
                    return new Tuple3<>("default first", 0, second.f1);
                }
            }
        }).print();
    }




    void crossFunction(ExecutionEnvironment env) throws Exception {
        List<String> data1 = new ArrayList<>();
        List<Integer> data2 = new ArrayList<>();
        data1.add("h");
        data1.add("c");
        data1.add("a");


        data2.add(11);
        data2.add(22);
        data2.add(44);
        DataSet<String> dataSet1 = env.fromCollection(data1);
        DataSet<Integer> dataSet2 = env.fromCollection(data2);

        dataSet1.cross(dataSet2).print();
    }
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        //new TransFormAPI().myMapFunction(executionEnvironment);
        //new TransFormAPI().myFilterFunction(executionEnvironment);
        //new TransFormAPI().myFilterFunction(executionEnvironment);
        //new TransFormAPI().myMapPartitionFunction(executionEnvironment);
        //new TransFormAPI().myFirstFunction(executionEnvironment);
        //new TransFormAPI().myFlatMapFunction(executionEnvironment);
        //new TransFormAPI().myDistinctFunction (executionEnvironment);
        //new TransFormAPI().myJoinFunction (executionEnvironment);
        // new TransFormAPI().myOutJoinFunction(executionEnvironment);
        new TransFormAPI().crossFunction(executionEnvironment);


    }
}
