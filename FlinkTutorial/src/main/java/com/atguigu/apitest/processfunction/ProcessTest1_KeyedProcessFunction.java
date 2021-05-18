package com.atguigu.apitest.processfunction;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * create by Shipeixin on  2021-05-18  14:57
 */
public class ProcessTest1_KeyedProcessFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("192.168.10.102", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        // 测试 KeyedProcessFunction  先分组
        dataStream.keyBy("id")
                .process( new MyProcess())
                .print();

        env.execute();
    }


    //实现自定义处理函数
    public static class MyProcess extends KeyedProcessFunction<Tuple,SensorReading,Integer>{

        // 定义状态
        private ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));

        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());

            //ctx
            ctx.timestamp();
            ctx.getCurrentKey();

            // 设置定时器，定时任务
            ctx.timerService().currentProcessingTime();
            ctx.timerService().currentWatermark();
            //  时间时间定时器  和 处理时间定时器
           // ctx.timerService().registerEventTimeTimer(  ctx.timerService().currentProcessingTime() + 1000);
            ctx.timerService().registerProcessingTimeTimer( ctx.timerService().currentProcessingTime() + 1000);
            timerState.update(ctx.timerService().currentProcessingTime() + 1000);

           // ctx.timerService().deleteEventTimeTimer();

            ctx.timerService().deleteProcessingTimeTimer(timerState.value());

        }


        // 定时器触发内容
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp + "定时器触发");
        }

        @Override
        public void close() throws Exception {
            timerState.clear();
        }
    }
}
