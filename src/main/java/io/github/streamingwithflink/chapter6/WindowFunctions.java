package io.github.streamingwithflink.chapter6;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 窗口函数
 * 窗口函数定义了针对窗口内元素的计算逻辑，可用于窗口的函数类型有两种：
 * 1. 增量聚合函数：窗口内以状态形式存储某个值，并需要根据每个加入窗口的元素对该值进行更新。
 * 2. 全量窗口函数: 收集窗口内的所有元素，并在执行计算时对他们进行遍历
 *
 * 一般有：
 * 增量聚合函数：
 * 1. ReduceFunction
 * 2. AggregateFunction
 * 全量窗口函数：
 * 3. ProcessWindowFunction
 *
 * @author yitian
 */
public class WindowFunctions {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sensorData = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        // ReduceFunction
        // 在WindowedStream上应用reduce lambda函数
        // 计算每15s的最低温度
        DataStream<SensorReading> minTempPerWindow = sensorData
                .keyBy(r -> r.id)
                .timeWindow(Time.seconds(15))
                .reduce((r1,r2) -> {
                    if (r1.temperature > r2.temperature) {
                        return r2;
                    } else {
                        return r1;
                    }
                });

        // 将sensorReading处理为Tuple2进行实现上述相同功能
//        DataStream<Tuple2<String, Double>> minTempPerWindow1 = sensorData
//                .map(r -> new Tuple2<>(r.id, r.temperature))
//                .keyBy(r -> r.f0)
//                .timeWindow(Time.seconds(15))
//                .reduce((r1, r2) -> {
//                    if (r1.f1 > r2.f1) {
//                        return r2;
//                    } else {
//                        return r1;
//                    }
//                });

        // 使用自定义算子，实现上述相同功能
        sensorData.keyBy(r -> r.id)
                .timeWindow(Time.seconds(15))
                .reduce(new MinTemperatureReducer());

        // AggregateFunction
        // 使用AggregateFunction计算每个窗口内传感器读数的平均温度
        // ...


        // ProcessWindowFunction
        // 使用ProcessWindowFunction计算每个传感器在每个窗口(5s)内的最低和最高温度
        DataStream<MinMaxTemp> minMaxTempPerWindow = sensorData.keyBy(r -> r.id)
                .timeWindow(Time.seconds(5))
                .process(new HighAndLowTempProcessFunction());
    }

    private static class MinTemperatureReducer implements ReduceFunction<SensorReading> {

        @Override
        public SensorReading reduce(SensorReading r1, SensorReading r2) throws Exception {
            if (r1.temperature > r2.temperature) {
                return r2;
            } else {
                return r1;
            }
        }
    }

    /**
     * 使用ProcessWindowFunction计算每个传感器在每个时间窗口内的最低温度和最高温度读数
     */
    private static class HighAndLowTempProcessFunction
            extends ProcessWindowFunction<SensorReading, MinMaxTemp, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<SensorReading> iterable,
                            Collector<MinMaxTemp> collector) throws Exception {
            List<Double> readingList = new ArrayList<>();
            for (SensorReading r : iterable) {
                readingList.add(r.temperature);
            }

            Double minTemp = readingList.stream().min((a, b) -> (int) (a - b)).get();
            Double maxTemp = readingList.stream().max((a, b) -> (int) (a - b)).get();
            Long windowEnd = context.window().getEnd();

            collector.collect(new MinMaxTemp(s, minTemp, maxTemp, windowEnd));
        }
    }

    private static class MinMaxTemp {
        String id;
        Double min;
        Double max;
        Long endTs;

        public MinMaxTemp() {}

        public MinMaxTemp(String id, Double min, Double max, Long endTs) {
            this.id = id;
            this.min = min;
            this.max = max;
            this.endTs = endTs;
        }
    }
}
