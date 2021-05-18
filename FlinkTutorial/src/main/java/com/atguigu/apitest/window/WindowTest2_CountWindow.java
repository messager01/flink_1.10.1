package com.atguigu.apitest.window;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * create by Shipeixin on  2021-05-14  13:36
 */
public class WindowTest2_CountWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("192.168.10.102", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        //  开计数窗口测试

        SingleOutputStreamOperator<Double> res = dataStream.keyBy(SensorReading::getId)
                .countWindow(10, 2)
                .aggregate(new MyAvgAggregate());

        res.print("res");

        env.execute();

    }

    public static class MyAvgAggregate implements AggregateFunction<SensorReading, Tuple2<Double,Integer>,Double> {

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorReading sensorReading, Tuple2<Double, Integer> calculator) {
            return new Tuple2<>(calculator.f0 + sensorReading.getTemperature(), calculator.f1 + 1 );
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> doubleIntegerTuple2) {
            return doubleIntegerTuple2.f0 / doubleIntegerTuple2.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> acc0, Tuple2<Double, Integer> acc1) {
            return new Tuple2<>(acc0.f0 + acc1.f0, acc0.f1 + acc1.f1);
        }
    }

}
