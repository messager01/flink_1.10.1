package com.atguigu.apitest.processfunction;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * create by Shipeixin on  2021-05-18  16:14
 */
public class ProcessTest3_SideOutputCase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("192.168.10.102", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //  测试 processFunction  自定义侧输出流操作

        // 定义侧输出流tag
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("low-temp") {
        };

        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                if (value.getTemperature() > 30){
                    // 30 为高温流
                    out.collect(value);
                }else {
                    // 侧输出流
                    ctx.output(outputTag, value);
                }
            }
        });

        highTempStream.print("high");
        highTempStream.getSideOutput(outputTag).print("low");



        env.execute();
    }

}
