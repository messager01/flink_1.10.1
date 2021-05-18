package com.atguigu.apitest.window;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * create by Shipeixin on  2021-05-14  15:14
 */
public class WindowTest3_EventTimeWindow {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置流式时间语义               事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStream<String> inputStream = env.socketTextStream("192.168.10.102", 7777);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })


                /**
                *  如果 数据到达的顺序比较理想（都是升序  ），不会乱序，  使用 AscendingTimestampExtractor 因此也无需设置 watermark
                * */
               /* .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
                    @Override
                    public long extractAscendingTimestamp(SensorReading element) {
                        return element.getTimestamp() * 1000;
                    }
                })*/


                /**
                *    数据的到达可能是乱序，使用 BoundedOutOfOrdernessTimestampExtractor 来设置 watermark
                * */
                // 时间戳提取器，定义了 事件时间语义，则必须指明  事件的时间   单位为ms            // 设置 watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        // 单位为 ms   因此需要 * 1000
                       return element.getTimestamp() * 1000;
                    }
                });


                // 基于事件时间的开窗聚合     统计 15s内温度的最小值

        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .minBy("temperature");

        minTempStream.print("minTemp");

    }

}
