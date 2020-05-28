package io.github.streamingwithflink.chapter6;

import io.github.streamingwithflink.chapter1.AverageSensorReadings;
import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 窗口算子
 * 新建一个窗口算子需要制定两个窗口组件
 * 1. 一个用于决定输入流中的元素如何划分的窗口分配器（Window Assigner）
 * 2. 一个作用于WindowedStream（或AllWindowedStream）上，用于处理分配到窗口中元素的窗口函数（WindowFunction）
 *
 * 内置窗口分配器包括：
 * 1. 滚动窗口：
 *      滚动事件时间窗口分配器：TumblingEventTimeWindows
 *      滚动处理时间窗口分配器：TumblingProcessingTimeWindows
 *    接收一个参数：以时间单位表的固定窗口大小
 *
 * 2. 滑动窗口分配器
 *      滑动事件时间窗口分配器
 *      滑动处理时间窗口分配器
 *    接收两个参数：a 窗口大小，b 窗口滑动间隔
 *
 *
 * @author yitian
 */
public class WindowOperator {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sensorData = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        // 使用滚动窗口分配器
        DataStream<SensorReading> alertedStream = tumblingWindowsAssinger(sensorData);
        // 使用滑动窗口分配器
//        alertedStream = slidingWindowAssigner(sensorData);
        // 使用会话窗口分配器
//        alertedStream = sessionWindowAssigner(sensorData);

        alertedStream.print();
        env.execute("WindowOperator");
    }

    /**
     * 滚动窗口分配器
     * 1. 滚动事件时间窗口分配器
     * 2. 滚动处理时间窗口分配器
     * @param sensorData 输入数据流
     * @return windowedStream
     */
    private static DataStream<SensorReading> tumblingWindowsAssinger(DataStream<SensorReading> sensorData) {
        // 滚动事件时间窗口分配器，apply(WindowFunction)
        DataStream<SensorReading> avgTemp = sensorData.keyBy(r -> r.id)
                // 定义滚动窗口分配器
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .apply(new AverageSensorReadings.TemperatureAverager());

        // 滚动事件时间窗口分配器，process(ProcessWindowFunction)
//        avgTemp = sensorData.keyBy(r -> r.id)
//                // 定义滚动窗口分配器
//                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
//                .process(new TemperatureWindowAverager());

        // 滚动事件时间窗口分配器的简写形式
//        avgTemp = sensorData.keyBy(r -> r.id)
//                // 该方法是对window.(TumblingEventTimeWindows.of(size))或window.(TumblingProcessingTimeWindows.of(size))的简写形式
//                // 具体调用哪个取决于配置的时间特性
//                .timeWindow(Time.seconds(1))
//                .process(new TemperatureWindowAverager());

        // 滚动事件时间窗口分配器设置起始偏移量
//        avgTemp = sensorData.keyBy(r -> r.id)
//                // 滚动窗口，可以设置滚动的起始偏移量，默认与197-01-01 00:00:00 进行对齐
//                .window(TumblingEventTimeWindows.of(Time.hours(1), Time.minutes(15)))
//                .process(new TemperatureWindowAverager());

        return avgTemp;
    }

    /**
     * 滑动窗口分配器
     * 1. 滑动事件时间窗口分配器
     * 2. 滑动处理时间窗口分配器
     * @param sensorData 输入数据流
     * @return windowedStream
     */
    private static DataStream<SensorReading> slidingWindowAssigner(DataStream<SensorReading> sensorData) {
        // 事件时间滑动窗口分配器
        DataStream<SensorReading> avgTemp = sensorData.keyBy(r -> r.id)
                // 定义滑动窗口分配器
                // 包含两个量：窗口固定大小，窗口滑动间隔
                // 每隔15分钟创建1小时的事件时间窗口
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(15)))
                .process(new TemperatureWindowAverager());

        // 处理时间滑动窗口分配器
//        avgTemp = sensorData.keyBy(r -> r.id)
//                // 每隔15分钟创建1小时的处理时间窗口
//                .window(SlidingProcessingTimeWindows.of(Time.hours(1), Time.minutes(15)))
//                .process(new TemperatureWindowAverager());

        // 滑动窗口的简写形式
//        avgTemp = sensorData.keyBy(r -> r.id)
//                .timeWindow(Time.hours(1), Time.minutes(15))
//                .process(new TemperatureWindowAverager());

        return avgTemp;
    }

    /**
     * 会话窗口分配器
     * 会话窗口将元素放入长度可变且不重叠的窗口中
     * 会话窗口的边界由非活动间隔，即持续没有收到记录的时间间隔来定义
     * @param sensorData 输入数据流
     * @return windowedStream
     */
    private static DataStream<SensorReading> sessionWindowAssigner(DataStream<SensorReading> sensorData) {
        // 会话窗口事件时间分配器
        DataStream<SensorReading> sessionWindows = sensorData.keyBy(r -> r.id)
                // 创建15分钟间隔的事件时间会话窗口
                .window(EventTimeSessionWindows.withGap(Time.minutes(15)))
                .process(new TemperatureWindowAverager());

        // 处理时间会话窗口分配器
//        sessionWindows = sensorData.keyBy(r -> r.id)
//                .window(ProcessingTimeSessionWindows.withGap(Time.minutes(15)))
//                .process(new TemperatureWindowAverager());

        return sessionWindows;
    }

    public static class TemperatureWindowAverager
            extends ProcessWindowFunction<SensorReading, SensorReading, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<SensorReading> iterable,
                            Collector<SensorReading> collector) throws Exception {
            int cnt = 0;
            double sum = 0.0;
            for (SensorReading r : iterable) {
                cnt ++;
                sum += r.temperature;
            }
            double avgTemp = sum / cnt;

            collector.collect(new SensorReading(s, context.window().getEnd(), avgTemp));
        }
    }
}
