package com.donaldy.demo.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWrodCountDemo {

    public static void main(String[] args) throws Exception {

        // 获取一个执行环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        // 读取输入数据
        DataSet<String> dataSet = environment.fromElements("");

        // 单词词频统计
        DataSet<Tuple2<String, Integer>> sum = dataSet.flatMap(new LineSplitter())
                .groupBy(0)
                .sum(1);

        sum.print();
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word : line.split(" ")) {

                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
