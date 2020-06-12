package io.github.streamingwithflink.chapter7;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 有状态算子：联合列表状态UnionListState
 * 该状态会在进行恢复时，需要被完整地复制到每个任务实例上
 * 实现CheckpointedFunction接口
 *
 * @author yitian
 */
public class FlatMapFunctionWithUnionListState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sensorData = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<SensorReading> keyedSensorData = sensorData.keyBy(r -> r.id);

        DataStream<Tuple3<String, Long, Long>> alerts = keyedSensorData
                .flatMap(new HighTempCounter(1.7));

        alerts.print();
        env.execute("FlatMapFunctionWithUnionListState");

    }

    /**
     * 实现CheckpointedFunction接口
     * 该函数分别利用键值分区状态和算子状态，来统计每个键值分区内和每个算子实例内有多少个传感器读数超出阈值
     */
    private static class HighTempCounter
            implements CheckpointedFunction, FlatMapFunction<SensorReading, Tuple3<String, Long, Long>> {

        // 温度阈值参数
        private Double threshold;

        // 本地存储算子实例高温数目的计数值
        private Long opHighTempCnt = 0L;

        private ValueState<Long> keyedCntState;

        private ListState<Long> opCntState;

        public HighTempCounter(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Long, Long>> collector) throws Exception {
            // 检查温度是否超过阈值
            if (sensorReading.temperature > threshold) {
                // 更新本地算子实例的高温计数器
                opHighTempCnt += 1;

                // 更新键值分区的高温计数器
                if (keyedCntState.value() == null) {
                    keyedCntState.update(0L);
                }
                Long keyHighTempCnt = keyedCntState.value() + 1;
                keyedCntState.update(keyHighTempCnt);

                // 发出新的计数器值
                collector.collect(new Tuple3<>(sensorReading.id, keyHighTempCnt, opHighTempCnt));
            }
        }

        /**
         * 该方法在创建CheckPointedFunction的并行实例时被调用，其触发的时机是应用启动或由于故障重启任务
         * @param functionInitializationContext 可以利用该参数对象，访问它的OperatorStateStore和KeyedStateStore对象，
         *                                      这两个状态存储对象能够使用Flink运行时来注册函数状态并返回状态对象
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
            // 初始化键值分区状态
            ValueStateDescriptor<Long> keyCntDescriptor = new ValueStateDescriptor<Long>("keyedCnt", Types.LONG);
            keyedCntState = functionInitializationContext.getKeyedStateStore().getState(keyCntDescriptor);

            // 初始化算子状态
            ListStateDescriptor<Long> opCntDescriptor = new ListStateDescriptor<Long>("opCnt", Types.LONG);
            opCntState = functionInitializationContext.getOperatorStateStore().getListState(opCntDescriptor);

            // 利用算子状态初始化本地的变量
            Iterable<Long> opCntStateList = opCntState.get();
            opHighTempCnt = 0L;
            for (Long item : opCntStateList) {
                opHighTempCnt += item;
            }
        }

        /**
         * 该方法会在生成检查点之前调用，它需要接受一个FunctionSnapshotContext对象作为参数
         * 该方法的目的是确保检查点开始之前的所有状态对象都已更新完成
         * @param functionSnapshotContext 从该传入对象中，可以获取检查点编号以及jobManager在初始化检查点时的时间戳
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            // 利用本地的状态更新算子状态
            opCntState.clear();
            opCntState.add(opHighTempCnt);
        }
    }
}
