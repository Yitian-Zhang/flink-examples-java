package io.github.streamingwithflink.chapter7;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;

/**
 * 有状态算子：算子状态（算子列表状态）
 *
 * @author yitian
 */
public class FlatMapFunctionWithListedCheckpoint {

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

        // 在每个函数并行实例内，统计该分区数据超过某个阈值的温度值数目
        DataStream<Tuple2<Integer, Long>> alerts = keyedData
                .flatMap(new HighTempCounter(70.0));

        alerts.print();

        env.execute("FlatMapFunctionWithListedCheckpoint");
    }

    /**
     * 使用算子列表状态的RichFlatMapFunction算子
     */
    private static class HighTempCounter extends RichFlatMapFunction<SensorReading, Tuple2<Integer, Long>>
            implements ListCheckpointed<Long> {
        // 设置的温度阈值参数
        private Double threshold;

        // 子任务的索引号
        private Integer subTaskIdx;

        // 本地变量计数器
        private Long highTempCnt = 0L;

        public HighTempCounter(Double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            subTaskIdx = getRuntimeContext().getIndexOfThisSubtask();
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple2<Integer, Long>> collector) throws Exception {
            if (sensorReading.temperature > this.threshold) {
                // 超出阈值，计数器+1
                highTempCnt += 1;
                collector.collect(new Tuple2<>(subTaskIdx, highTempCnt));
            }
        }

        /**
         * 该方法会在Flink触发为有状态函数生成检查点时调用
         * @param l 一个唯一且单调递增的检查点编号checkpointId
         * @param l1 一个JobManager开始创建检查点的机器时间timestamp
         * @return 以列表的形式返回算子的状态
         * @throws Exception
         */
        @Override
        public List<Long> snapshotState(long l, long l1) throws Exception {
            // 将一个包含单个数目值的列表作为状态快照
            return Collections.singletonList(highTempCnt);
        }

        /**
         * 该方法在初始化函数状态时调用，该过程可能发生在作业启动（无论是否从savepoint启动），或在故障情况下恢复
         * @param list 接收的状态对象列表
         * @throws Exception
         */
        @Override
        public void restoreState(List<Long> list) throws Exception {
            // 将状态恢复为列表中的全部long值之和
            highTempCnt = 0L;
            for (Long cnt : list) {
                highTempCnt += cnt;
            }
        }
    }
}
