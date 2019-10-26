package com.wen.flink.scala.demo.batch.file

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object FileOperator {
  val env = ExecutionEnvironment.getExecutionEnvironment

  def readRecursiveFiles(): Unit = {
    val filePath="C:\\data\\txt"
    env.readTextFile(filePath).print()
    println("----------------------------------------")
    var parameter=new Configuration
    parameter.setBoolean("recursive.file.enumeration", true)
    env.readTextFile(filePath).withParameters(parameter).print()

  }

  def main(args: Array[String]): Unit = {
    readRecursiveFiles()

  }
}
