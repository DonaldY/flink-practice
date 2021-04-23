package com.donaldy.demo.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author donald
 * @date 2021/04/23
 */
public class StateDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> data = env.fromElements(Tuple2.of(1L, 3L),
                Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L));
        KeyedStream<Tuple2<Long, Long>, Long> keyed = data.keyBy(value -> value.f0);

        // 按照 key 分组策略，对流式数据调用状态化处理
        SingleOutputStreamOperator<Tuple2<Long, Long>> flatMaped =
                keyed.flatMap(new RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
            private transient ValueState<Tuple2<Long, Long>> sum;

            // 更新 state
            @Override
            public void flatMap(Tuple2<Long, Long> value,
                                Collector<Tuple2<Long, Long>> out) throws Exception {
                // 获取当前状态值
                Tuple2<Long, Long> currentSum = sum.value();
                // 更新
                currentSum.f0 += 1;
                currentSum.f1 += value.f1;

                System.out.println("...currentSum: " + currentSum);

                // 更新状态值
                sum.update(currentSum);

                // 如果count >= 2 清空状态值, 重新计算
                if (currentSum.f0 >= 5) {
                    out.collect(new Tuple2<>(value.f0, currentSum.f1 / currentSum.f0));
                    sum.clear();
                }
            }

            // 做出 state
            @Override
            public void open(Configuration parameters) {
                ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
                        "average",
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}),
                        Tuple2.of(0L, 0L)
                );

                ValueStateDescriptor<Tuple2<Long, Long>> descriptor1 = new
                        ValueStateDescriptor<>("average", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                }));

                sum = getRuntimeContext().getState(descriptor);
            }
        });
        flatMaped.print();
        env.execute();
    }
}