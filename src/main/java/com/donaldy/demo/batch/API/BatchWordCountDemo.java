package com.donaldy.demo.batch.API;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCountDemo {

    public static void main(String[] args) throws Exception {

        // 获取一个执行环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        // 读取输入数据
        // flatMap: 1进n出
        DataSet<String> dataSet = environment.fromElements("flink flink flink", "spark spark spark");

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

    /**
     *
     * MapPartition: 类似map, 一次处理一个分区的数据
     * （如果在进行map处理的时候需要获取第三方资源, 建议使用MapPartition）
     *
     * 使用场景：
     * 过滤脏数据，数据清洗等
     */
    public void mapPartition() throws Exception {

        // 获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 产生数据
        DataSet<Long> dataSet = env.generateSequence(1, 20);

        dataSet.mapPartition(new MyMapPartitioinFunction());

        dataSet.print();
    }

    public static class MyMapPartitioinFunction implements MapPartitionFunction<Long, Long> {

        @Override
        public void mapPartition(Iterable<Long> iterable, Collector<Long> collector) throws Exception {
            long count = 0;

            for (Long value : iterable) {

                ++count;
            }

            collector.collect(count);
        }
    }

    /**
     * Transfamation-filter
     *
     * 含义：数据筛选（满足条件event的被筛选出来进行后续处理），
     * 根据 FilterFunction返回的布尔值来判断是否保留元素，true为保留，false则丢弃
     *
     * 使用场景：
     * 过滤脏数据、数据清洗等
     *
     * 聚合操作：
     * Reduce：对数据进行聚合操作，结合当前元素和上一次reduce返回的值进行聚合操作，然后返回一个新的值。
     *
     * aggregate： sum、max、min等
     */


}
