package io.github.streamingwithflink.chapter5;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

/**
 * Flink流式应用并行度设置
 *  方法1：执行环境级别设置
 *  方法2：单个算子级别设置
 * 默认情况下，应用内的所有算子的并行度都会被设置为应用执行环境的并行度
 *  1. 本地环境：CPU线程数
 *  2. Flink集群：集群默认并行度
 * 一般情况下，最好将算子并行度设置为随环境默认并行度变化的值
 *
 * 功能说明:
 * Flink应用中三种并行度设置方式
 *
 * @author yitian
 */
public class ParallelismSetting {
    public static void main(String[] args) {
        // 获取当前执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 并行度设置1
        setExecutionEnvDefaultParallelism(env);

        // 并行度设置2
//        setCustomEnvParallelism(env);

        // 并行度设置3
//        setOperatorParallelism(env);

    }

    private static void setExecutionEnvDefaultParallelism(StreamExecutionEnvironment env) {
        // 1. 获取通过集群配置或提交客户端指定的默认并行度
        int defaultParallelism = env.getParallelism();
        System.out.printf("The default parallelism is : " + defaultParallelism);
    }

    private static void setCustomEnvParallelism(StreamExecutionEnvironment env) {
        // 2. 也可以覆盖环境的默认并行度，一旦覆盖，便不能通过提交客户端的方式控制应用的并行度
        // 设置环境的并行度
        env.setParallelism(32);
    }

    /**
     * 当使用提交客户端上传应用并将并行度设置为16时，下面数据流中的数据源算子会以16的并行度运行，
     * map算子会以32的并行度运行，print数据汇算子会以2的并行的运行
     *
     * 如果在一个8core的本地机器上以本地环境运行该数据流，则数据源并行度为8，map并行度为16，
     * print算子并行度为2
     */
    private static void setOperatorParallelism(StreamExecutionEnvironment env) {
        // 3. 可以通过显式指定的方式来覆盖某个算子的并行度
        // 获取env默认并行度
        int defaultParallelism = env.getParallelism();

        // 数据源以默认的并行度运行
        DataStream<SensorReading> reandings = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());


        DataStreamSink<Tuple2<String, Double>> result = reandings
                // 设置map算子的并行度为默认并行度的2倍
                .map(r -> new Tuple2<String, Double>(r.id, r.temperature)).setParallelism(defaultParallelism * 2)
                // 设置print数据汇算子的并行度为2
                .print().setParallelism(2);
    }
}
