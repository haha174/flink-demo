package com.wen.flink.scala.demo.streaming.world_count

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time


object StreamingWorldCountDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.socketTextStream("10.148.185.205", 9999)
    import org.apache.flink.api.scala._

    source.flatMap(_.toLowerCase().split(" "))
      .filter(_.nonEmpty).map(word => (word, 1))
      .keyBy(0).timeWindow(Time.seconds(5)).sum(1).print().setParallelism(1)

    env.execute()
  }
}
