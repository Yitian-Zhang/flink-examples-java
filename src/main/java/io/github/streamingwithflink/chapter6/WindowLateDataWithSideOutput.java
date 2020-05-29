package io.github.streamingwithflink.chapter6;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 迟到事件的重定向处理，使用sideOutputLateData方法
 *
 * @author yitian
 */
public class WindowLateDataWithSideOutput {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sensorData = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        // 在窗口算子中为迟到事件定义副输出
        SingleOutputStreamOperator<Tuple4<String, Long, Long, Integer>> countPer10Secs = sensorData.keyBy(r -> r.id)
                .timeWindow(Time.seconds(10))
                .sideOutputLateData(new OutputTag<SensorReading>("late-readings"))
                .process(new CountFunction());

        countPer10Secs.getSideOutput(new OutputTag<SensorReading>("late-readings"));
        countPer10Secs.print();

        // 用于过滤出迟到的传感器读数并将其重定向到副输出的ProcessFunction
        SingleOutputStreamOperator<SensorReading> filteredReadings = sensorData.process(new LateReadingsFilter());

        // 获取迟到数据
        filteredReadings.getSideOutput(new OutputTag<SensorReading>("late-readings"));
        filteredReadings.print();

        env.execute("WindowLateDataWithSideOutput");

    }


    public static class CountFunction extends ProcessWindowFunction<SensorReading, Tuple4<String, Long, Long, Integer>, String, TimeWindow> {
        @Override
        public void process(String id, Context ctx, Iterable<SensorReading> readings, Collector<Tuple4<String, Long, Long, Integer>> out) throws Exception {
            // count readings
            int cnt = 0;
            for (SensorReading r : readings) {
                cnt++;
            }
            // get current watermark
            long evalTime = ctx.currentWatermark();
            // emit result
            out.collect(Tuple4.of(id, ctx.window().getEnd(), evalTime, cnt));
        }
    }

    /**
     * 用于过滤迟到的传感器读数，并将其重定向到副输出的ProcessFunction
     */
    private static class LateReadingsFilter extends ProcessFunction<SensorReading, SensorReading> {
        OutputTag<SensorReading> lateReadingsOut = new OutputTag<>("late-readings");

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {
            // 比较记录时间戳和当前水位线
            if (sensorReading.timestamp < context.timerService().currentWatermark()) {
                // 将迟到读书重定向到副输出
                context.output(lateReadingsOut, sensorReading);
            } else {
                // 非迟到数据，正常输出
                collector.collect(sensorReading);
            }
        }
    }
}
