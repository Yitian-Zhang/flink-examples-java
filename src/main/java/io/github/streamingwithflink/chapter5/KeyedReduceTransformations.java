package io.github.streamingwithflink.chapter5;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 功能说明：
 * KeyedStream流中实现reduce操作示例
 *
 * 测试说明：
 * Tested Done
 *
 * @author yitian
 */
public class KeyedReduceTransformations {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List l1 = new ArrayList();
        l1.add("tea");
        List l2 = new ArrayList();
        l2.add("vin");
        List l3 = new ArrayList();
        l3.add("cake");

        DataStream<Tuple2<String, List<String>>> inputStream = env.fromElements(
                Tuple2.of("en", l1),
                Tuple2.of("fr", l2),
                Tuple2.of("en", l3));

        // 将相同键值的流中的数据，放到一个list集合中
        DataStream<Tuple2<String, List<String>>> resultStream = inputStream
                .keyBy(0)
                .reduce((r1, r2) -> {
                    List tmp = new ArrayList();
                    tmp.add(r1.f1);
                    tmp.add(r2.f1);
                    return new Tuple2<>(r1.f0, tmp);
                });

        resultStream.print();
        // output:
        // 7> (fr,[vin])
        // 5> (en,[tea])
        // 5> (en,[[tea], [cake]])

        env.execute("KeyedReduceTransformations");
    }
}
