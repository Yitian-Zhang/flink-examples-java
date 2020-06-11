package io.github.streamingwithflink.chapter6;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flink处理函数
 * 处理函数相对于普通DataStream API的用户自定义函数，它可以访问记录的时间戳和水位线，并支持注册在将来某个特定时间触发的计时器
 * 此外，处理函数的副输出功能允许将记录发送到多个输出流中。
 * 处理函数常被用于构建事件驱动型应用，或实现一些内置窗口及转换无法实现的自定义逻辑。FlinkSql中的大部分算子都是利用处理函数实现的。
 *
 * Flink提供了8种不同的处理函数：
 * 1. ProcessFunction
 * 2. KeyedProcessFunction
 * 3. CoProcessFunction
 * 4. ProcessJoinFunction
 * 5. BroadcastProcessFunction
 * 6. KeyedBroadcastProcessFunction
 * 7. ProcessWindowFunction
 * 8. ProcessAllWindowFunction
 *
 * Tested Done
 *
 * @author yitian
 */
public class KeyedProcessFunctionTimers {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sensorData = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        // 监测温度，如果某个sensor的温度在1秒的处理时间内持续上升则发出警告
        DataStream<String> warnings = sensorData.keyBy(r -> r.id)
                // 使用TempIncreaseAlertFunction来监测温度的上升
                .process(new TempIncreaseAlertFunction());

        warnings.print();
        env.execute("KeyedProcessFunctionTimers");

    }

    private static class TempIncreaseAlertFunction extends KeyedProcessFunction<String, SensorReading, String> {

        // 存储最近一次传感器温度读数
        private ValueState<Double> lastTemp;

        // 存储当前活动计时器的时间戳
        private ValueState<Long> currentTimer;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("lastTemp", Types.DOUBLE));
            currentTimer = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timer", Types.LONG));
        }

        @Override
        public void processElement(SensorReading sensorReading, Context context,
                                   Collector<String> collector) throws Exception {
            // 获取前一个温度，需要处理状态初始值
            Double prevTemp = lastTemp.value() == null ? 0.0 : lastTemp.value();
            // 更新最近一次的温度
            lastTemp.update(sensorReading.temperature);

            // 获取当前计时器时间戳状态，并处理状态初始值
            Long curTimerTimestamp = currentTimer.value() == null ? 0L : currentTimer.value();
            if (prevTemp == 0.0 || sensorReading.temperature < prevTemp) {
                // 温度下降，删除当前计时器
                context.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
                currentTimer.clear();
            } else if (sensorReading.temperature > prevTemp && curTimerTimestamp == 0) {
                // 温度升高并且还未设置计时器
                // 以当前时间+1s设置处理时间计时器
                Long timerTs = context.timerService().currentProcessingTime() + 1000;
                // 注册处理时间计时器
                context.timerService().registerProcessingTimeTimer(timerTs);

                // 记录当前的计时器
                currentTimer.update(timerTs);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("Temperature of sensor '" + ctx.getCurrentKey() + "' monotonically increased for 1 second.");
            currentTimer.clear();
        }
    }
}
