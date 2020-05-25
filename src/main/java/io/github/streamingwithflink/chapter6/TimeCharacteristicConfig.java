package io.github.streamingwithflink.chapter6;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * 配置时间特性
 * 1. 时间戳
 * 2. 水位线
 *
 * @author yitian
 */
public class TimeCharacteristicConfig {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 使用事件时间，同时支持：处理时间（ TimeCharacteristic.ProcessingTime）
        // 摄入时间（TimeCharacteristic.IngestionTime）
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 每10s生成一次水位线，默认为200ms
        env.getConfig().setAutoWatermarkInterval(1000L);

        // 设置数据源和时间戳和水位线分配器
        DataStream<SensorReading> senosrData = setPeriodicWatermarksAssigner(env);

        // 开始进行数据流处理
        // ...

        env.execute("TimeCharacteristicConfig");
    }

    /**
     * 自定义周期性水位线分配器，通过追踪至今为止所遇到的最大元素的时间戳来生成水位线
     */
    private static DataStream<SensorReading> setPeriodicWatermarksAssigner(StreamExecutionEnvironment env) {
        // 1. 自定义周期性时间戳分配器
        DataStream<SensorReading> sensorData = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new PeriodAssigner());

        // 2. 内置周期性水位线时间戳分配器：assignAscendingTimestamps
        // 当输入的元素时间戳是单调递增的，则该方法使用当前时间戳生成水位线
//        sensorData = env.addSource(new SensorSource())
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
//                    @Override
//                    public long extractAscendingTimestamp(SensorReading sensorReading) {
//                        return sensorReading.timestamp;
//                    }
//                });

        // 3. 内置周期性水位线时间戳分配器：BoundedOutOfOrdernessTimestampExtractor
        // 明确知道输入流中的延迟时使用
//        env.addSource(new SensorSource())
//                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(10)) {
//                    @Override
//                    public long extractTimestamp(SensorReading sensorReading) {
//                        return sensorReading.timestamp;
//                    }
//                });

        return sensorData;
    }

    /**
     * 设置定点水位线分配器
     * 根据输入元素生成水位线
     */
    private static DataStream<SensorReading> setPunctuatedWatermarksAssigner(StreamExecutionEnvironment env) {
        DataStream<SensorReading> sensorData = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new PunctuatedAssigner());

        return sensorData;
    }

    private static class PeriodAssigner implements AssignerWithPeriodicWatermarks<SensorReading> {
        // 1分钟的毫秒数
        private Long bound = 60 * 1000L;

        // 观察到的最大时间戳
        private Long maxTs = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            // 生成具有1分钟容忍度的水位线
            return new Watermark(maxTs - bound);
        }

        @Override
        public long extractTimestamp(SensorReading sensorReading, long l) {
            // 更新最大的时间戳数值
            maxTs = maxTs > sensorReading.timestamp ? maxTs : sensorReading.timestamp;
            // 返回记录的时间戳
            return sensorReading.timestamp;
        }
    }

    /**
     * 定点水位线分配器实现，实现AssignerWithPunctuatedWatermarks接口
     * 该接口中的checkAndGetNextWatermark方法会在针对每个事件的extractTimestamp方法后调用，
     * 决定是否生成一个新的水位线
     *
     * 下面的实现示例，会根据从id为sensor_1的传感器收到所有的读数后产生水位线
     */
    private static class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<SensorReading> {
        // 1分钟的毫秒数
        private Long bound = 60 * 1000L;

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(SensorReading sensorReading, long l) {
            if (sensorReading.id.equals("sensor_1")) {
                // 如果当前事件来自sensor_1传感器，则发出一条水位线
                return new Watermark(l - bound);
            }
            // 不发出水位线
            return null;
        }

        @Override
        public long extractTimestamp(SensorReading sensorReading, long l) {
            // 为事件记录生成时间戳
            return sensorReading.timestamp;
        }
    }
}
