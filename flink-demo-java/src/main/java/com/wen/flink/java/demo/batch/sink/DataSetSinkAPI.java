package com.wen.flink.java.demo.batch.sink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

public class DataSetSinkAPI {
    static ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

    void sinkToLocalText() throws Exception {
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(i);
        }
        DataSet<Integer> dataSet1 = executionEnvironment.fromCollection(data);
        dataSet1 .writeAsText("C:\\data\\txt\\tmp\\sinkToLocalText.txt", FileSystem.WriteMode.OVERWRITE) ;
        executionEnvironment.execute("sinkToLocalText");
    }

    public static void main(String[] args) throws Exception {
        new DataSetSinkAPI().sinkToLocalText();
    }
}
