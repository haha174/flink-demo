package com.wen.flink.scala.demo.batch.world_count

import org.apache.flink.api.scala.ExecutionEnvironment

object WorldCountDemo {
  def main(args: Array[String]): Unit = {
    val input = "C:\\Users\\wenqchen\\Desktop\\data\\test.txt"
    val env = ExecutionEnvironment.getExecutionEnvironment
    var lines=env.readTextFile(input)

    import org.apache.flink.api.scala._
    lines.flatMap(line=>line.split(" ")).filter(_.nonEmpty)
        .map(word => (word, 1))
       .groupBy(0).sum(1).print()

  }
}