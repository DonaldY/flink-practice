package com.deepmind.flink

import org.apache.flink.api.scala._

/**
  * @author donald
  * @date 2021/04/15
  */
object WordCountBatch {

  def main(args: Array[String]): Unit = {
    val inputPath = ""
    val outputPath = ""
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val text: DataSet[String] = environment.readTextFile(inputPath)
    val out: AggregateDataSet[(String, Int)] = text.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
    out.writeAsCsv(outputPath, "\n", " ").setParallelism(1)
    environment.execute("word count batch")
  }
}
