package com.atguigu.apitest.window;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * create by Shipeixin on  2021-05-14  11:12
 */
public class WindowTest1_TimeWindow {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 并行度设置1，方便看结果
        env.setParallelism(1);

//        // 从文件读取数据
//        DataStream<String> dataStream = env.readTextFile("/tmp/Flink_Tutorial/src/main/resources/sensor.txt");

        // 从socket文本流获取数据
        DataStream<String> inputStream = env.socketTextStream("192.168.10.102", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 开窗测试

   /*     // 1. 增量聚合函数 (这里简单统计每个key组里传感器信息的总数)
        DataStream<Integer> resultStream = dataStream.keyBy(SensorReading::getId)
//                .countWindow(10, 2);
//               .window(EventTimeSessionWindows.withGap(Time.minutes(1)));
//               .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
//               .timeWindow(Time.seconds(15)) // 已经不建议使用@Deprecated
              .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
             .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {

                    // 新建的累加器
                  @Override
                   public Integer createAccumulator() {
                       return 0;
                    }

                    // 每个数据在上次的基础上累加
                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    // 返回结果值
                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    // 分区合并结果(TimeWindow一般用不到，SessionWindow可能需要考虑合并)
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

        resultStream.print("result");*/



   /**
   *  全窗口函数
   * */

        SingleOutputStreamOperator<Tuple3<String,Long,Integer>> res = dataStream.keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .apply(new WindowFunction<SensorReading, Tuple3<String,Long,Integer>, String, TimeWindow>() {

                    @Override
                    public void apply(String s, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String id = s;
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        Long windowEnd = window.getEnd();
                        out.collect(new Tuple3<>(s,windowEnd,count));
                    }
                });

        res.print("全窗口函数");

        env.execute();

    }

}
