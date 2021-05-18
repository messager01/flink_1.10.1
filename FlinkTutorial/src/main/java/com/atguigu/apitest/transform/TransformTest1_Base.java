package com.atguigu.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * create by Shipeixin on  2021-05-11  13:58
 */
public class TransformTest1_Base {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("F:\\学习代码\\尚硅谷\\大数据\\Flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        // 1. map  把String转换成长度输出
        DataStream<Integer> mapStream = inputStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });

        // 2. flatMap   按 ， 切分字段
        DataStream<String> flatMap = inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] fields = s.split(",");
                for (String field : fields) {
                    collector.collect(field);
                }
            }
        });


        // 3. filter   筛选sensor_1 开头对应的数据
        DataStream<String> filter = inputStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("sensor_1");
            }
        });


        // 打印输出
        mapStream.print("map");
        flatMap.print("flatMap");
        filter.print("filter");


        env.execute();
    }

}
