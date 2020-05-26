package io.github.streamingwithflink.chapter6;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.lang.reflect.Type;

/**
 * 使用处理函数向副输出发送数据
 * Flink中处理函数的副输出功能，允许从同一个函数中发出多条数据流，且它们的类型可以不同。
 * 每个副输出都由一个OutputTag<T>对象标识，其中T是副输出结果流的类型
 * 处理函数可以使用context对象将记录发送至一个或多个副输出。
 *
 * @author yitian
 */
public class ProcessFunctionWithSideOutput {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sensorData = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        // 监控冷冻温度数据流
        SingleOutputStreamOperator<SensorReading> monitoredReadings = sensorData.
                process(new FreezingMonitor());

        // 获取并打印包含冷冻警报的副输出
        // 注意：需要明确指明OutputTag中的Type
        monitoredReadings.getSideOutput(new OutputTag<String>("freezing-alarms", Types.STRING))
                .print();

        // 打印主输出
        sensorData.print();
        // output:
        // 4> Freezing Alarm for sensor_35
        // 5> (sensor_49, 1590304728027, 45.043991152872444)

        env.execute("ProcessFunctionWithSideOutput");
    }


    /**
     * 向副输出发送记录的ProcessFunction
     * 对于温度低于32F的读数，向副输出发送冷冻警报
     */
    private static class FreezingMonitor extends ProcessFunction<SensorReading, SensorReading> {

        // 定义副输出标签
        // 注意：需要明确指明OutputTag中的Type
        private OutputTag<String> freezingAlarmOutput = new OutputTag<String>("freezing-alarms", Types.STRING);

        @Override
        public void processElement(SensorReading sensorReading, Context context,
                                   Collector<SensorReading> collector) throws Exception {
            // 如果温度低于32F，则发出冷冻警报
            if (sensorReading.temperature < 32.0) {
                context.output(freezingAlarmOutput, String.format("Freezing Alarm for %s", sensorReading.id));
            }

            // 将所有读数发送到常规主输出
            collector.collect(sensorReading);
        }
    }
}
