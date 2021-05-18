package com.atguigu.apitest.state;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * create by Shipeixin on  2021-05-18  11:49
 */
public class StateTest3_KeyedStateApplicationCase {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("192.168.10.102", 7777);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个 flatMap操作。检测温度跳变检测报警，相邻的两个温度差超过十度就报警
        DataStream<Tuple3<String,Double,Double>> warningStream = dataStream.keyBy(SensorReading::getId)
                .flatMap(new TempChangeWarning(10.0));

        warningStream.print();

        env.execute();

    }


    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String,Double,Double>>{

        private Double warningTemperature;

        // 定义状态   上次温度的值
        private ValueState<Double> lastTempareture;

        public TempChangeWarning(Double warningTemperature) {
            this.warningTemperature = warningTemperature;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempareture = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp", Double.class));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            // 获取状态
            Double lastTemp = lastTempareture.value();
            if (lastTemp != null ) {
                Double diff = Math.abs(value.getTemperature() - lastTemp);
                if (diff >= warningTemperature) {
                    out.collect(new Tuple3<>(value.getId(), value.getTemperature(), lastTemp));
                }

            }
            // 更新 state 的值
            lastTempareture.update(value.getTemperature());
        }


        @Override
        public void close() throws Exception {
            lastTempareture.clear();
        }
    }

}
