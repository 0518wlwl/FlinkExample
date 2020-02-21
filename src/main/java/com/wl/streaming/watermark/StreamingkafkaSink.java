package com.wl.streaming.watermark;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import java.util.Properties;

/*
 * kafka sink
 */
public class StreamingkafkaSink {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();

        //设置最大并行度
        env.setMaxParallelism(10);

        //每隔1000 ms进行启动一个检查点，【设置checkpoint的周期】
        env.enableCheckpointing(1000);
        //高级选项
        //设置模式exactly-once（这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //确保检查检查点之间至少500ms的间隔 【checkpoint的最小时间】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(6000);
        //同一时间只允许一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //表示一旦Flink处理程序被cancel后，会保留checkpoint的数据，以便根据需求恢复到指定的check
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        DataStreamSource<String> text  = env.socketTextStream("hadoop103", 9001, "\n");

        String brokerList="hadoop102:9092";
        String topic = "t1";
        //FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011(brokerList, topic, new SimpleStringSchema());

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",brokerList);

        //第一种解决方案 设置 FlinkKafkaProducer011 超时时间
        prop.setProperty("transaction.timeout.ms",60000*15+"");
        //使用仅一次语意 kafka producer
        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(topic, new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()), prop, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);
        text.addSink(myProducer);
        env.execute("StreamingkafkaSink");


    }
}
