package io.github.streamingwithflink.chapter7;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 有状态算子：键值分区状态
 *
 * 应用一个带有键值分区ValueState的FlatMapFunction
 * 该示例应用会在监测到相邻温度值变化超过给定阈值时发出警报
 *
 * @author yitian
 */
public class FlatMapFunctionWithValueState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        // 数据源
        DataStream<SensorReading> sensorData = env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        // 创建分区流
        DataStream<SensorReading> keyedData = sensorData
                .keyBy(r -> r.id);

        // 调用带有键值分区ValueState的FlatMapFunction算子
        DataStream<Tuple3<String, Double, Double>> alerts = keyedData
                .flatMap(new TemperatureAlertFunction(1.7));


        alerts.print();

        env.execute("FlatMapFunctionWithValueState");
    }

    /**
     * 实现带有键值分区ValueState的FlatMapFunction
     * 作用：温度变化超出设定阈值发出警报
     */
    public static class TemperatureAlertFunction extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        // 触发警报阈值
        private Double threshold;

        // 状态引用对象
        private ValueState<Double> lastTempState;

        public TemperatureAlertFunction(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Double> lastTempDescriptor = new ValueStateDescriptor<Double>("lastTemp", Types.DOUBLE);
            // 获得状态引用
            lastTempState = getRuntimeContext().getState(lastTempDescriptor);
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            // 从状态中获取上一次的温度
            Double lastTemp = lastTempState.value();
            System.out.println("Current SensorReading is: " + sensorReading.toString());
            System.out.println("Current Last Temperature is: " + lastTemp);

            // 解决开始lastTemp=null异常情况
            if (lastTemp == null) {
                lastTemp = sensorReading.temperature;
            }

            // 检查是否需要触发警报
            Double tempDiff = Math.abs(sensorReading.temperature - lastTemp);
            if (tempDiff > threshold) {
                //  温度变化超过阈值
                collector.collect(new Tuple3<>(sensorReading.id, sensorReading.temperature, tempDiff));
            }
            // 更新lastTemp状态
            this.lastTempState.update(sensorReading.temperature);
        }
    }
}
