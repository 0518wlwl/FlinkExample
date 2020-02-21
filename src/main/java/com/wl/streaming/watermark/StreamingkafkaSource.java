package com.wl.streaming.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

/*
 * kafka source
 */
public class StreamingkafkaSource {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();

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



        String topic = "t1";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","hadoop102:9092");
        prop.setProperty("group.id","con1");
        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<String>(topic, new SimpleStringSchema(), prop);

        DataStreamSource<String> text = env.addSource(myConsumer);

        text.print().setParallelism(1);

        env.execute("StreamingkafkaSource");


    }
}
