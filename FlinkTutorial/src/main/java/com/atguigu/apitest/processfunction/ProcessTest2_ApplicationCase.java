package com.atguigu.apitest.processfunction;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * create by Shipeixin on  2021-05-18  15:42
 */
public class ProcessTest2_ApplicationCase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("192.168.10.102", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.keyBy("id")
            .process(new TempConsIncreWarning(10L));


        env.execute();
    }


    /**
    *   实现自定义处理函数，如果一段时间内温度连续上升，则发出警告
     *   此处不能简单的开窗，无论是滚动窗口还是滑动窗口都不行，因为无法确定开窗时间
     *   因此可以使用定时器，当某一时刻监测到温度上升，则开启一个窗口
    * */
    public static class TempConsIncreWarning extends KeyedProcessFunction<Tuple,SensorReading,String>{

        private Long interval;

        public TempConsIncreWarning(Long interval) {
            this.interval = interval;
        }


        // 定义状态，  1.上次的温度    2.定时器创建时间
        private ValueState<Double> tempState;

        private ValueState<Long> timerState;

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Tuple currentKey = ctx.getCurrentKey();
            out.collect("传感器" + currentKey.getField(0) + "连续" + interval + "s持续上升" );
            timerState.clear();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 获取状态控制句柄
            tempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp-state", Double.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-state", Long.class));
        }



        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double lastTemp = tempState.value();
            if (lastTemp == null){
                tempState.update(Double.MIN_VALUE);
                lastTemp = Double.MIN_VALUE;
            }
            Long timeerTs = timerState.value();
            // 如果当前温度上升并且没有定时器，注册十秒后的定时器，开始等待
            if (value.getTemperature() > lastTemp && timeerTs == null){
                //计算出定时器时间戳
                long ts = ctx.timerService().currentProcessingTime() + interval * 1000;
                ctx.timerService().registerProcessingTimeTimer(ts);
                timerState.update(ts);
            }else if (value.getTemperature() < lastTemp && timeerTs != null){
                //如果温度下降删除定时器
                ctx.timerService().deleteProcessingTimeTimer(timeerTs);
                timerState.clear();
            }



            // 更新温度状态
            tempState.update(value.getTemperature());
        }


        @Override
        public void close() throws Exception {
            super.close();
        }

    }

}
