package jian.wu.processfunction;

import jian.wu.source.ClickSource;
import jian.wu.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Auther: sanctuary
 * @Date: 2023/2/18 22:20
 * @Description: 基础processtime测试定时器
 */
public class ProcessTimeTimer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.keyBy(event -> true).process(new MyProcessFunction()).print();
        env.execute();
    }

    private static class MyProcessFunction extends KeyedProcessFunction<Boolean, Event, String> {

        @Override
        public void processElement(Event value, KeyedProcessFunction<Boolean, Event, String>.Context ctx, Collector<String> out) throws Exception {
            long curTime = ctx.timerService().currentProcessingTime();
            System.out.println("current event processing time is: " + curTime);
            ctx.timerService().registerProcessingTimeTimer(curTime + 10 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Boolean, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            System.out.println("timer triggering:" + timestamp);
        }
    }
}
