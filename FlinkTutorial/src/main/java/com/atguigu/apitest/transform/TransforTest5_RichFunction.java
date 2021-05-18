package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * create by Shipeixin on  2021-05-11  16:32
 */
public class TransforTest5_RichFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从文件读取
        DataStream<String> inputStream = env.readTextFile("F:\\学习代码\\尚硅谷\\大数据\\Flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        // 转换成sensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        DataStream<Tuple2<String,Integer>> resultStream = dataStream.map(new MyMapper());


        resultStream.print();
        env.execute();
    }


    // 实现自定义的富函数
    public static class MyMapper extends RichMapFunction<SensorReading,Tuple2<String,Integer>>{

        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            return new Tuple2<>(sensorReading.getId(),getRuntimeContext().getIndexOfThisSubtask());
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            //  初始化工作    一般是定义状态 或者 建立数据库连接
            super.open(parameters);
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            //  一般用来清空状态   或者关闭连接
            super.close();
            System.out.println("close");
        }
    }

}
