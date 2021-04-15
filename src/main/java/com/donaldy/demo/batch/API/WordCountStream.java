package com.donaldy.demo.batch.API;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author donald
 * @date 2021/04/15
 */
public class WordCountStream {

    public static void main(String[] args) throws Exception {

        // 监听的ip和端口号
        String ip = "127.0.0.1";
        int port = 7788;

        // 获取 Flink 流执行环境
        StreamExecutionEnvironment streamExecutionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取socket输入数据
        DataStreamSource<String> textStream =
                streamExecutionEnvironment.socketTextStream(ip, port, "\n");
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator
                = textStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
                String[] splits = s.split("\\s");
                for (String word : splits) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> word = tuple2SingleOutputStreamOperator
                .keyBy(0)
                .timeWindow(Time.seconds(2), Time.seconds(1))
                .sum(1);

        // 打印数据
        word.print();

        // 触发任务执行
        streamExecutionEnvironment.execute("wordcount stream process");
    }
}
