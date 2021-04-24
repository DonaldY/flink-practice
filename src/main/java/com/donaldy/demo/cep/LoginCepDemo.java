package com.donaldy.demo.cep;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @author donald
 * @date 2021/04/23
 */
public class LoginCepDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<LoginBean> data = env.fromElements(new LoginBean(1L, "fail", 1597905234000L),
                new LoginBean(1L, "success", 1597905235000L),
                new LoginBean(2L, "fail", 1597905236000L),
                new LoginBean(2L, "fail", 1597905237000L),
                new LoginBean(2L, "fail", 1597905238000L),
                new LoginBean(3L, "fail", 1597905239000L),
                new LoginBean(3L, "success", 1597905240000L));


        // 2. 在数据源上作出 watermark
        SingleOutputStreamOperator<LoginBean> watermarks =
                data.assignTimestampsAndWatermarks(new WatermarkStrategy<LoginBean>() {
                    @Override
                    public WatermarkGenerator<LoginBean> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<LoginBean>() {
                            long maxTimeStamp = Long.MIN_VALUE;

                            @Override
                            public void onEvent(LoginBean event, long eventTimestamp, WatermarkOutput output) {
                                maxTimeStamp = Math.max(maxTimeStamp, event.getTs());
                            }

                            long maxOutOfOrderness = 500L;

                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {
                                output.emitWatermark(new Watermark(maxTimeStamp - maxOutOfOrderness));
                            }
                        };
                    }
                }.withTimestampAssigner((element, recordTimestamp) -> element.getTs()));

        // 3. 在 watermark 上根据 id 分组 keyby
        KeyedStream<LoginBean, Long> keyedStream = watermarks.keyBy(LoginBean::getId);

        // 4. 做出模式 Pattern
        Pattern<LoginBean, LoginBean> pattern = Pattern.<LoginBean>begin("start").where(new IterativeCondition<LoginBean>() {
            @Override
            public boolean filter(LoginBean loginBean, Context<LoginBean> context) {
                return loginBean.getState().equals("fail");
            }
        }).next("next").where(new IterativeCondition<LoginBean>() {
            @Override
            public boolean filter(LoginBean loginBean, Context<LoginBean> context) {
                return loginBean.getState().equals("fail");
            }
        }).within(Time.seconds(5));


        // 5. 在数据流上进行模式匹配
        PatternStream<LoginBean> patternStream = CEP.pattern(keyedStream, pattern);

        // 6. 提取匹配成功的数据
        SingleOutputStreamOperator<String> process = patternStream.process(new PatternProcessFunction<LoginBean, String>() {
            @Override
            public void processMatch(Map<String, List<LoginBean>> match, Context ctx,
                                     Collector<String> out) {
                List<LoginBean> start = match.get("start");
                List<LoginBean> next = match.get("next");
                String res = "start:" + start + "...next:" + next;
                out.collect(res + start.get(0).getId());
            }
        });

        process.print();

        env.execute();
    }
}
