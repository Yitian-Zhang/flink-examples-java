package io.github.streamingwithflink.chapter6;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 使用延迟容忍度（allowed lateness）来处理迟到读数（数据）
 *
 * @author yitian
 */
public class WindowLateDataWithAllowedLateness {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sensorData = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<Tuple4<String, Long, Integer, String>> countPer10Secs = sensorData.keyBy(r -> r.id)
                .timeWindow(Time.seconds(10))
                // 额外处理5秒的迟到读数
                .allowedLateness(Time.seconds(5))
                // 如果遇到迟到读数，则重新计数并更新结果
                .process(new UpdatingWindowCountFunction());

        countPer10Secs.print();
        env.execute("WindowLateDataWithAllowedLateness");
    }

    /**
     * 用于计数的WindowProcessFunction，会区分首次计算结果和后续更新
     */
    private static class UpdatingWindowCountFunction
            extends ProcessWindowFunction<SensorReading, Tuple4<String, Long, Integer, String>, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<SensorReading> iterable,
                            Collector<Tuple4<String, Long, Integer, String>> collector) throws Exception {
            // count readings
            int cnt = 0;
            for (SensorReading r : iterable) {
                cnt++;
            }

            // 该状态用于标识是否是第一次对窗口进行计算
            ValueState<Boolean> isUpdate = context.windowState().getState(
                    new ValueStateDescriptor<>("isUpdate", Types.BOOLEAN));

            if (!isUpdate.value()) {
                // 首次计算并发出结果
                collector.collect(new Tuple4<>(s, context.window().getEnd(), cnt, "first"));
            } else {
                // 并非首次计算，发出更新
                collector.collect(new Tuple4<>(s, context.window().getEnd(), cnt, "update"));
            }
        }
    }
}
