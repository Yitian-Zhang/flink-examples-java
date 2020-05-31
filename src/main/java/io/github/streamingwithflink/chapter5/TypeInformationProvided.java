package io.github.streamingwithflink.chapter5;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink类型系统的核心类是TypeInformation，它为系统生成序列化器和比较器提供了必要的信息
 * Flink为Java和Scala提供两个辅助类，java中是：org.apache.flink.api.common.typeinfo.Types
 * 其中的静态方法可以用来生成TypInformation
 *
 * @author yitian
 */
public class TypeInformationProvided {

    public static void main(String[] args) {
        // 原始类型的TypeInformation
        TypeInformation<Integer> intType = Types.INT;

        // java元组的TypeInformation
        TypeInformation<Tuple2<Long, String>> tupleType = Types.TUPLE(Types.LONG, Types.STRING);

        // POJO的TypeInformation
        TypeInformation<SupportedTypes.Person> personType = Types.POJO(SupportedTypes.Person.class);


        // 显示提供类型信息（大部分情况下Flink可以自动推断类型并生成正确的TypeInformation），主要由两种方法
        // 1. 通过实现ResultTypeQuerable接口来扩展函数，在其中提供返回类型的TypeInformation
        // 2. 在定义Dataflow时使用Java DataStream API中的returns方法来显示指定算子的返回类型

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<Tuple2<String, Integer>> persons = env.fromElements(
                Tuple2.of("Adam", 17),
                Tuple2.of("Sarah", 23));

        DataStream<SupportedTypes.Person> resut = persons
                .map(r -> new SupportedTypes.Person(r.f0, r.f1))
                .filter(r -> r.getAge() > 18)
                // 为结果显示指明返回的对象类型信息（TypeInformation）
                .returns(Types.POJO(SupportedTypes.Person.class));
    }
}
