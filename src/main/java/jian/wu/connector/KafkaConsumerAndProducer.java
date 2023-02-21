package jian.wu.connector;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @Auther: sanctuary
 * @Date: 2023/2/20 14:59
 * @Description: 从kafka读取信息发送到kafka
 *
 */
public class KafkaConsumerAndProducer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setTopics("dwd_traffic_page_log")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kfSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        kfSource.print();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.21.69:9092");

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "dwd_traffic_page_log",
                new SimpleStringSchema(),
                properties
        );
        // 4 生产者和 flink 流关联
        kfSource.addSink(kafkaProducer);
        // 5 执行
        env.execute();
    }
}
