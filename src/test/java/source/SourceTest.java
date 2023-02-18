package source;

import jian.wu.source.ClickSource;
import jian.wu.source.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

/**
 * @Auther: sanctuary
 * @Date: 2023/2/18 19:21
 * @Description:
 */
public class SourceTest {
    @Test
    public void TestSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());
        streamSource.print(">>>");
        env.execute();
    }
}
