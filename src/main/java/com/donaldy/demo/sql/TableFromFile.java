package com.donaldy.demo.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author donald
 * @date 2021/04/24
 */

public class TableFromFile {
    public static void main(String[] args) throws Exception {
        // 设置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 定义初始化表环境的参数
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .withBuiltInCatalogName("default_catalog")
                .withBuiltInDatabaseName("default_database")
                .build();
        // 创建表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 从文件系统获取数据，并建表
        tEnv.connect(new FileSystem()
                .path("data/wc.txt"))
                .withSchema(new Schema().field("name", DataTypes.STRING()))
                .createTemporaryTable("nameTable");
        // 查询数据
        String sql = "select * from nameTable";
        Table resultTable = tEnv.sqlQuery(sql);
        tEnv.sqlQuery(sql);
        // 转化查询结果为DataStream
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tEnv.toRetractStream(resultTable, Row.class);
        // 输出打印
        tuple2DataStream.print();
        env.execute();
    }
}
