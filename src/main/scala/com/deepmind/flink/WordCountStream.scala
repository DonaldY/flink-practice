package com.deepmind.flink

import org.apache.flink.streaming.api.scala._

/**
  * @author donald
  * @date 2021/04/15
  *
  * 流式数据
  *
  * 控制台输出： nc -lp 7788
  */
object WordCountStream {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val streamData: DataStream[String] = environment.socketTextStream("127.0.0.1", 7788)
    val out = streamData.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)
    out.print()
    environment.execute()
  }
}
