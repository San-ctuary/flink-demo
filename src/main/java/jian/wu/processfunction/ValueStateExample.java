package jian.wu.processfunction;

import jian.wu.source.ClickSource;
import jian.wu.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Auther: sanctuary
 * @Date: 2023/2/19 10:45
 * @Description: 根据用户id分流， 每隔10s统计一次用户pv结果
 */
public class ValueStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        stream.print("source:");
        stream.keyBy(event -> event.user)
                .process(new MyProcessFunction()).print();
        env.execute();
    }



    private static class MyProcessFunction extends KeyedProcessFunction<String, Event, String> {
        private ValueState<Long> pvState;
        private ValueState<Long> timeState;
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("timer trigger: " + ctx.getCurrentKey() + " total pv is : " + pvState.value() + " set time : " + timeState.value() + " current time is :" + timestamp);
            pvState.clear();
            timeState.clear();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> valueStateDescriptor = new ValueStateDescriptor<>("pv state", Long.class);
            ValueStateDescriptor<Long> timeStateDescriptor = new ValueStateDescriptor<>("time state", Long.class);
            pvState = getRuntimeContext().getState(valueStateDescriptor);
            timeState = getRuntimeContext().getState(timeStateDescriptor);
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            Long cnt = pvState.value();
            if (cnt == null) {
                cnt = 0L;
            }
            pvState.update(cnt + 1);
            Long curTime = ctx.timestamp();
            if (timeState.value() == null) {
                ctx.timerService().registerEventTimeTimer(curTime + 10 * 1000L);
                timeState.update(curTime);
            }
        }
    }
}
