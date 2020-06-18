package io.github.streamingwithflink.chapter7;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 有状态算子：广播状态（Broadcast State）
 * 实现用广播流动态配置阈值的温度报警应用
 *
 * @author yitian
 */
public class KeyedStreamWithBroadcastState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sensorData = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<ThresholdUpdate> thresholds = env.fromElements(
                new ThresholdUpdate("sensor_1", 5.0d),
                new ThresholdUpdate("sensor_2", 10.0d),
                new ThresholdUpdate("sensor_1", 8.5d)
        );

        DataStream<SensorReading> keyedSensorData = sensorData.keyBy(r -> r.id);

        // 广播状态的描述符
        MapStateDescriptor<String, Double> broadcastStateDescriptor = new MapStateDescriptor<String, Double>(
                "thresholds", Types.STRING, Types.DOUBLE);
        // 使用描述符创建广播数据流
        BroadcastStream<ThresholdUpdate> broadcastThresholds = thresholds.broadcast(broadcastStateDescriptor);

        // 联结键值分区传感器数据和广播的规则流
        DataStream<Tuple3<String, Double, Double>> alerts = keyedSensorData.connect(broadcastThresholds)
                .process(new UpdatableTemperatureAlertFunction());

        alerts.print();

        env.execute("KeyedStreamWithBroadcastState");

    }

    /**
     * 实现KeyedBroacastProcessFunction，支持运行时动态配置传感器阈值数据
     */
    private static class UpdatableTemperatureAlertFunction
            extends KeyedBroadcastProcessFunction<String, SensorReading, ThresholdUpdate, Tuple3<String, Double, Double>> {

        // 广播状态的描述符
        private MapStateDescriptor<String, Double> thresholdDescriptor = new MapStateDescriptor<String, Double>(
                "thresholds", Types.STRING, Types.DOUBLE);

        // 键值分区状态引用对象
        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Double> lastTempDescriptor = new ValueStateDescriptor<Double>("lastTemp", Types.DOUBLE);
            // 获取键值分区状态引用对象
            lastTempState = getRuntimeContext().getState(lastTempDescriptor);
        }

        /**
         * 处理事件流数据（这里就是SensorData数据），传入的是只读的上下文ReadOnlyContext
         */
        @Override
        public void processElement(SensorReading sensorReading, ReadOnlyContext readOnlyContext,
                                   Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            // 获取只读的广播状态
            ReadOnlyBroadcastState<String, Double> thresholds = readOnlyContext.getBroadcastState(thresholdDescriptor);

            // 检查阈值是否已经存在
            if (thresholds.contains(sensorReading.id)) {
                // 获取指定传感器的阈值
                Double sensorThreshold = thresholds.get(sensorReading.id);
                // 从状态中获取上一次的温度
                Double lastTemp = lastTempState.value();

                if (lastTemp == null) {
                    lastTemp = sensorReading.temperature;
                }

                // 检查是否需要发出警报
                Double tempDiff = Math.abs(sensorReading.temperature - lastTemp);
                if (tempDiff > sensorThreshold) {
                    // 温度增加超出阈值
                    collector.collect(new Tuple3<>(sensorReading.id, sensorReading.temperature, tempDiff));
                }
            }

            this.lastTempState.update(sensorReading.temperature);
        }

        /**
         * 处理广播的数据（规则流数据，这里就是ThresholdStream流中的数据），传入可读写的上下文Context
         */
        @Override
        public void processBroadcastElement(ThresholdUpdate thresholdUpdate, Context context,
                                            Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            // 获取广播状态引用对象
            BroadcastState<String, Double> thresholds = context.getBroadcastState(thresholdDescriptor);

            if (thresholdUpdate.getThreshold() != 0.0d) {
                // 为指定传感器配置新的阈值
                thresholds.put(thresholdUpdate.getSensorId(), thresholdUpdate.getThreshold());
            } else {
                // 删除该传感器的阈值
                thresholds.remove(thresholdUpdate.getSensorId());
            }
        }
    }
}
