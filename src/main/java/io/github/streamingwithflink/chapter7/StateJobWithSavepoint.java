package io.github.streamingwithflink.chapter7;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 为有状态应用添加保护点机制（savepoint），增加应用的可维护性
 * 两个重要参数：
 *  1. 算子唯一标识符
 *  2. 算子最大并行度
 *
 * @author yitian
 */
public class StateJobWithSavepoint {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        env.enableCheckpointing(10000L);

        DataStream<SensorReading> sensorData = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<SensorReading> keyedSensorData = sensorData.keyBy(r -> r.id);

        DataStream<Tuple3<String, Double, Double>> alerts = keyedSensorData
                .flatMap(new FlatMapFunctionWithValueState.TemperatureAlertFunction(1.1))
                // 使用uid方法指定算子的唯一标识
                .uid("TempAlert")
                //  设置算子的最大并行度，会覆盖应用级别的数值
                .setMaxParallelism(1024);

        alerts.print();
        env.execute("StateJobWithSavepoint");
    }
}
