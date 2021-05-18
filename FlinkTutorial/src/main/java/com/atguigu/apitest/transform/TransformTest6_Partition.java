package com.atguigu.apitest.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * create by Shipeixin on  2021-05-11  16:47
 */
public class TransformTest6_Partition {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<String> inputStream = env.readTextFile("F:\\学习代码\\尚硅谷\\大数据\\Flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        inputStream.print("input");

        DataStream<String> shuffleInputStream = inputStream.shuffle();

        shuffleInputStream.print("shuffle");

        inputStream.global().print("global");

        env.execute();
    }
}
