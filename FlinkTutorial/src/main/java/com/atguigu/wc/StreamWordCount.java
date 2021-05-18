package com.atguigu.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * create by Shipeixin on  2021-05-08  15:29
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        //  创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        //env.setParallelism(10);

       /* String inputPath = "F:\\学习代码\\尚硅谷\\大数据\\Flink\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataStream<String> inputDataStream = env.readTextFile(inputPath);*/

       // 使用 parameter tool 读取 参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        Integer port = parameterTool.getInt("port");

       // 从 socket 中读取文件
        DataStream<String> inputDataStream = env.socketTextStream(host, port);


        // 基于数据流转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1).setParallelism(2);

        resultStream.print().setParallelism(1);

        // 执行任务
        env.execute();
    }

}
