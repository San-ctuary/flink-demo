package jian.wu.sql;

import jian.wu.pojo.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Auther: sanctuary
 * @Date: 2023/2/22 14:40
 * @Description: Flink sql 基础示例
 */
public class FlinkSqlDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);
        Table table = streamTableEnvironment.fromDataStream(eventStream);
//        以下两种方式都可以进行查询
//        streamTableEnvironment.createTemporaryView("NewTable", table);
//        Table visitTable = streamTableEnvironment.sqlQuery("select user, url from NewTable");
        Table visitTable = streamTableEnvironment.sqlQuery("select user, url from " + table);
        streamTableEnvironment.toDataStream(visitTable).print();
        env.execute();
    }
}
