package com.wen.flink.scala.demo.batch.data_source

import com.wen.flink.demo.domain.entity.Student
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object DataSourceApi {

  /**
    * 从集合中创建
    */
  def fromCollection(env: ExecutionEnvironment): Unit = {
    val data = 1 to 10
    env.fromCollection(data).print()
  }

  def fromCSV(env: ExecutionEnvironment): Unit = {
    val filePath = "C:\\data\\txt\\score\\test.csv"
    //env.readCsvFile[(Int,String,Int)](filePath=filePath,ignoreFirstLine = true).print()
      //env.readCsvFile[(Int,String)](filePath=filePath,ignoreFirstLine = true,includedFields = Array(0,1)).print()
    //case class MyCaseClass(id:Int,name:String,age:Int)
    //env.readCsvFile[MyCaseClass](filePath=filePath,ignoreFirstLine = true,includedFields = Array(1,2,3)).print()
    env.readCsvFile[Student](filePath, ignoreFirstLine = true,pojoFields = Array("id","name","age")).print()
  }

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //fromCollection(env)
    fromCSV(env);
  }


}
