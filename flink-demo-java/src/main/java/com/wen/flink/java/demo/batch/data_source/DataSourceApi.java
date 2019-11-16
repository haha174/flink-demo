package com.wen.flink.java.demo.batch.data_source;

import com.wen.flink.demo.domain.entity.Student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import scala.Tuple3;
import org.apache.flink.configuration.*;
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


    void fromCSV(ExecutionEnvironment env) throws Exception {
        String filePath="C:\\data\\txt\\score\\test.csv";
        env.readCsvFile(filePath).ignoreFirstLine().pojoType(Student.class,"id","name","age").print();
       // .types(Integer.class, String.class, Double.class);
    }

    void fromFilePath(ExecutionEnvironment env) throws Exception {
        String filePath="C:\\data\\txt\\score";
        env.readTextFile(filePath).print();
    }

    void readCompressionFile(ExecutionEnvironment env) throws Exception {
        String filePath="C:\\data\\txt\\score\\1.gzip";
        env.readTextFile(filePath).print();
    }



    void fromRecursiveFilePath(ExecutionEnvironment env) throws Exception {
        String filePath="C:\\data\\txt\\score";
        Configuration parameters = new Configuration();
        parameters.setBoolean("recursive.file.enumeration", true);
        env.readTextFile(filePath).withParameters(parameters).print();
    }
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment=ExecutionEnvironment.getExecutionEnvironment();
        //new DataSourceApi().fromCollection(executionEnvironment);
        //new DataSourceApi().fromFilePath(executionEnvironment);
        //new DataSourceApi().fromCSV(executionEnvironment);
        //new DataSourceApi().fromRecursiveFilePath(executionEnvironment);
       // new DataSourceApi().readCompressionFile(executionEnvironment);
    }
}
