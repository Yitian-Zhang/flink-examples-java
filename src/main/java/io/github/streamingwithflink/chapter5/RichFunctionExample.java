package io.github.streamingwithflink.chapter5;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * 富函数实现示例
 *
 * @author yitian
 */
public class RichFunctionExample extends RichFlatMapFunction<Integer, Tuple2<Integer, Integer>> {

    private int subTaskIndex = 0;

    // open方法在没给任务首次调用转化方法前调用一次，进行一些初始化的工作
    @Override
    public void open(Configuration parameters) throws Exception {
        subTaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        // 进行一些其他初始化的工作，例如和外部系统建立连接
    }

    @Override
    public void flatMap(Integer integer, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
        // 子任务的编号从0开始
        if (integer % 2 == subTaskIndex) {
            collector.collect(new Tuple2<>(subTaskIndex, integer));
        }
        // 进行额外的处理工作...
    }

    @Override
    public void close() throws Exception {
        // 进行一些清理工作，例如关闭外部系统和连接等...
    }
}
