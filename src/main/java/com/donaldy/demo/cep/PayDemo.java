package com.donaldy.demo.cep;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author donald
 * @date 2021/04/24
 */
public class PayDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.setParallelism(1);
        DataStreamSource<PayBean> data = environment.fromElements(
                new PayBean(1L, "create", 1597905234000L),
                new PayBean(1L, "pay", 1597905235000L),
                new PayBean(2L, "create", 1597905236000L),
                new PayBean(2L, "pay", 1597905237000L),
                new PayBean(3L, "create", 1597905239000L)
        );

        SingleOutputStreamOperator<PayBean> watermarks = data.assignTimestampsAndWatermarks(new WatermarkStrategy<PayBean>() {
            @Override
            public WatermarkGenerator<PayBean> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<PayBean>() {
                    long maxTimeStamp = Long.MIN_VALUE;
                    long maxOutOfOrderness = 500L;

                    @Override
                    public void onEvent(PayBean event, long eventTimestamp, WatermarkOutput output) {
                        maxTimeStamp = Math.max(maxTimeStamp, event.getTs());
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(maxTimeStamp - maxOutOfOrderness));
                    }
                };
            }
        }.withTimestampAssigner((element, recordTimestamp) -> element.getTs()));

        KeyedStream<PayBean, Long> keyedStream = watermarks.keyBy(PayBean::getId);

        Pattern<PayBean, PayBean> pattern = Pattern.<PayBean>begin("start").where(new IterativeCondition<PayBean>() {
            @Override
            public boolean filter(PayBean payBean, Context<PayBean> context) {
                return payBean.getState().equals("create");
            }
        }).followedBy("next").where(new IterativeCondition<PayBean>() {
            @Override
            public boolean filter(PayBean payBean, Context<PayBean> context) {
                return payBean.getState().equals("pay");
            }
        }).within(Time.seconds(600));

        PatternStream<PayBean> patternStream = CEP.pattern(keyedStream, pattern);

        OutputTag<PayBean> outoftime = new OutputTag<PayBean>("outoftime"){};

        SingleOutputStreamOperator<PayBean> result = patternStream.select(outoftime, new PatternTimeoutFunction<PayBean, PayBean>() {
            @Override
            public PayBean timeout(Map<String, List<PayBean>> map, long l) {
                return map.get("start").get(0);
            }
        }, new PatternSelectFunction<PayBean, PayBean>() {
            @Override
            public PayBean select(Map<String, List<PayBean>> map) {
                return map.get("start").get(0);
            }
        });

        DataStream<PayBean> sideOutput = result.getSideOutput(outoftime);

        sideOutput.print();

        environment.execute();
    }
}
