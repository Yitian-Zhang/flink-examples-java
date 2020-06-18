package io.github.streamingwithflink.chapter7;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 使用KeyedProcessFunction实现两个连续温度超出一定阈值，发出警报的功能
 * 此处加入了：在某一个键值超过1小时（时间时间）没有新到的测量数据时，将其对应的状态进行清除
 *
 * @author yitian
 */
public class KeyedStreamWithStateClean {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sensorData = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<Tuple3<String, Double, Double>> alerts = sensorData
                .keyBy(r -> r.id)
                // process上面必须有分区函数
                .process(new SelfCleaningTemperatureAlertFunction(1.7));

    }

    /**
     * 实现可以清楚状态的KeyedProcessFunction
     * 在某一个键值超过1小时（时间时间）没有新到的测量数据时，将其对应的状态进行清除
     */
    private static class SelfCleaningTemperatureAlertFunction
            extends KeyedProcessFunction<String, SensorReading, Tuple3<String, Double, Double>> {

        private Double threshold;

        // 用于存储最近一次温度的键值分区状态
        private ValueState<Double> lastTempState;

        // 前一个注册的计时器的键值分区状态
        private ValueState<Long> lastTimerState;

        public SelfCleaningTemperatureAlertFunction(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 注册用于最近一次稳定的状态
            ValueStateDescriptor<Double> lastTempDesc = new ValueStateDescriptor<Double>("lastTemp", Types.DOUBLE);
            lastTempState = getRuntimeContext().getState(lastTempDesc);

            // 注册用于前一个计时器的状态
            ValueStateDescriptor<Long> lastTimerDesc = new ValueStateDescriptor<Long>("lastTimer", Types.LONG);
            lastTimerState = getRuntimeContext().getState(lastTimerDesc);
        }

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            // 将清理状态的计时器设置为比记录时间戳晚1个小时
            Long newTimer = context.timestamp() + (3600 * 1000);
            // 获取当前计时器的时间戳
            Long curTimer = lastTimerState.value();

            // 删除前一个计时器并注册一个新的计时器
            context.timerService().deleteEventTimeTimer(curTimer);
            context.timerService().registerEventTimeTimer(newTimer);

            // 更新计时器时间戳状态
            lastTimerState.update(newTimer);

            // 从状态中获取上一次的温度
            Double lastTemp = lastTempState.value();
            // 检查是否需要发出警报
            Double tempDiff = Math.abs(sensorReading.temperature - lastTemp);
            if (tempDiff > threshold) {
                // 稳定增加超出阈值
                collector.collect(new Tuple3<>(sensorReading.id, sensorReading.temperature, tempDiff));
            }

            // 更新lastTemp状态
            this.lastTempState.update(sensorReading.temperature);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            // 清除当前键值的所有状态
            lastTempState.clear();
            lastTimerState.clear();
        }
    }
}