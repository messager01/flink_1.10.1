package com.atguigu.apitest.window;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * create by Shipeixin on  2021-05-14  14:47
 */
public class WindowTest2_LateData {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("", 7777);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        //  定义迟到标签
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        SingleOutputStreamOperator<SensorReading> sumStream = dataStream.keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                // 允许迟到一分钟   一分钟后窗口关闭
                .allowedLateness(Time.minutes(1))

                // 对于长时间迟到的数据，使用侧边流收集
                .sideOutputLateData(outputTag)
                .sum("temperature");

        //  单独收集的迟到数据，然后手动批处理合并
        DataStream<SensorReading> lateStream = sumStream.getSideOutput(outputTag);

    }

}
