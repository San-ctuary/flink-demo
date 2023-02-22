package jian.wu.sql;

import jian.wu.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Auther: sanctuary
 * @Date: 2023/2/22 15:35
 * @Description:
 */
public class FlinkSqlDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L),
                new Event("Bob", "./prod?id=3", 90 * 1000L),
                new Event("Alice", "./prod?id=7", 105 * 1000L));
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);
        stEnv.createTemporaryView("eventStream", stream);
        Table table = stEnv.sqlQuery("select * from eventStream");
        Table filterByName = stEnv.sqlQuery("select * from eventStream " +
                "where user = 'Alice'");
        stEnv.toDataStream(filterByName).print("Alice url>>>");
        Table groupByTable = stEnv.sqlQuery("select user, count(url) from eventStream " +
                "group by user");
        stEnv.toChangelogStream(groupByTable).print("group by>>>");
        env.execute();

    }
}
