package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * create by Shipeixin on  2021-05-11  14:54
 */
public class TransformTest3_Reduce {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取
        DataStream<String> inputStream = env.readTextFile("F:\\学习代码\\尚硅谷\\大数据\\Flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        // 转换成sensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        // 分组   取最大的温度值，以及当前最新的时间戳
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            //  t1 是之前聚合出的状态    t2 是新来的数据
            @Override
            public SensorReading reduce(SensorReading t1, SensorReading t2) throws Exception {

                return new SensorReading(t1.getId(), t2.getTimestamp(), Math.max(t1.getTemperature(), t2.getTemperature()));
            }
        });

        resultStream.print();
        env.execute();
    }
}
