package io.github.streamingwithflink.chapter5;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink支持的数据类型
 * 1. Java和Scala的原始数据类型
 * 2. Java和Scala元组
 * 3. Scala样例类
 * 4. POJO（）
 * 5. 一些特殊类型
 *
 * @author yitian
 */
public class SupportedTypes {
    public static void main(String[] args) {
        // 1. Java和Scala的原始数据类型
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<Long> numbers = env.fromElements(1L, 2L, 3L, 4L)
                .map(r -> r + 1);
        numbers.print();

        // 2. Java和Scala元组
        DataStream<Tuple2<String, Integer>> persons = env.fromElements(
                new Tuple2<>("Adam", 17),
                new Tuple2<>("Sarah", 23))
                .filter(r -> r.f1 > 18);
        persons.print();

        DataStream<Tuple2<String, Integer>> persons1 = env.fromElements(
                Tuple2.of("Adam", 17),
                Tuple2.of("Sarah", 23))
                .filter(r -> (int) r.getField(1) > 18);
        persons1.print();

        // Java中的元组时可变类型
        Tuple2<String, Integer> personTuple = Tuple2.of("Alex", 43);
        Integer age = personTuple.getField(1);
        System.out.printf("This person's age is: " + age);

        // 重新设置该元组中的第二个字段（0开始）
        personTuple.f1 = 43;
        personTuple.setField(1, 45);
        System.out.println("This person's age has been updated to : " + personTuple.f1);

        // 3. POJO
        DataStream<Person> personData = env.fromElements(
                new Person("Alex", 23),
                new Person("Jack", 56))
                .filter(r -> r.age > 18);
        personData.print();
    }

    /**
     * Java POJO
     * 1. 一个共有类
     * 2. 具有一个共有的无参构造方法
     * 3. 所有字段是共有的，或者具有对应的get set方法
     * 4. 所有字段类型必须是Flink支持的
     */
    public static class Person {
        private String name;
        private Integer age;

        public Person() {}

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }
    }


}
