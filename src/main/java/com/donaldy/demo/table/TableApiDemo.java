package com.donaldy.demo.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author donald
 * @date 2021/04/16
 */
public class TableApiDemo {

    public static void main(String[] args) throws Exception {

        // Flink 执行环境 env
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 用 env, 做出 Table 环境 tableEnvironment
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        // 获取流式数据源
        DataStreamSource<Tuple2<String, Integer>> data = executionEnvironment.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                while (true) {
                    ctx.collect(new Tuple2<>("name", 10));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
            }
        });
        // 将流式数据源做成Table
        Table table = tableEnvironment.fromDataStream(data, $("name"), $("age"));
        // 对Table中的数据做查询
        Table name = table.select($("name"));
        // 将处理结果输出到控制台
        DataStream<Tuple2<Boolean, Row>> result = tableEnvironment.toRetractStream(name, Row.class);
        result.print();
        executionEnvironment.execute();
    }
}
