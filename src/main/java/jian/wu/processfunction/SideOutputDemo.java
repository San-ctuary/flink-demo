package jian.wu.processfunction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Auther: sanctuary
 * @Date: 2023/2/16 14:35
 */
public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5);
        // 这里加入{}表示是继承的子类
        OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

        SingleOutputStreamOperator<Long> mainStream = integerDataStreamSource.process(new ProcessFunction<Integer, Long>() {
            @Override
            public void processElement(Integer value, ProcessFunction<Integer, Long>.Context ctx, Collector<Long> out) throws Exception {
                out.collect(Long.valueOf(value));
                ctx.output(outputTag, "side output : " + value);
            }
        });

        DataStream<String> sideOutput = mainStream.getSideOutput(outputTag);
        mainStream.print();
        sideOutput.print();
        env.execute();
    }
}
