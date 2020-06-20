package io.github.streamingwithflink.chapter7;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 为有状态的应用开启故障恢复（一致性检查点机制）
 *
 * @author yitian
 */
public class StateJobWithCheckpoint {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        // 手动开启一致性检查点机制，检查点的生成周期为10s（10000ms）
        env.enableCheckpointing(100000L);

        DataStream<SensorReading> sensorData = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        // 计算每15s的最低温度
        DataStream<SensorReading> result = sensorData.keyBy(r -> r.id)
                .timeWindow(Time.seconds(10))
                .reduce((r1, r2) -> {
                    if (r1.temperature > r2.temperature) {
                        return r2;
                    } else {
                        return r1;
                    }
                });

        result.print();
        env.execute("StateJobWithCheckpoint");
    }
}
