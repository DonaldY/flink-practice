package com.donaldy.demo.batch.API;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 实现了 Flink 中的 SourceFunction 接口，
 * 同时实现了其中的 run 方法，在 run 方法中每隔一秒钟随机发送一个自定义的 Item。
 *
 * @author donald
 * @date 2020/07/11
 */
public class MyStreamingSource implements SourceFunction<MyStreamingSource.Item> {

    private boolean isRunning = true;

    /**
     * 重写run方法产生一个源源不断的数据发送源
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Item> ctx) throws Exception {
        while(isRunning){
            Item item = generateItem();
            ctx.collect(item);

            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }
    @Override
    public void cancel() {
        isRunning = false;
    }

    //随机产生一条商品数据
    private Item generateItem(){
        int i = new Random().nextInt(100);

        Item item = new Item();
        item.setName("name" + i);
        item.setId(i);
        return item;
    }

    class Item{
        private String name;
        private Integer id;

        Item() {
        }

        public String getName() {
            return name;
        }

        void setName(String name) {
            this.name = name;
        }

        private Integer getId() {
            return id;
        }

        void setId(Integer id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return "Item{" +
                    "name='" + name + '\'' +
                    ", id=" + id +
                    '}';
        }
    }
}


class StreamingDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取数据源
        // 注意：并行度设置为1
        DataStreamSource<MyStreamingSource.Item> text =
                env.addSource(new MyStreamingSource()).setParallelism(1);

        DataStream<MyStreamingSource.Item> item = text.map(
                (MapFunction<MyStreamingSource.Item, MyStreamingSource.Item>) value -> value);

        //打印结果
        item.print().setParallelism(1);

        String jobName = "user defined streaming source";

        env.execute(jobName);
    }

}
