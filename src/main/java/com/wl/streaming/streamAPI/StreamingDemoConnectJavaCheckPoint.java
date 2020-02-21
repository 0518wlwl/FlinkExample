package com.wl.streaming.streamAPI;

import com.wl.streaming.custormSource.MyParalleSource;
import com.wl.streaming.custormSource.StreamingDemoWithMyNoPralalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class StreamingDemoConnectJavaCheckPoint {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        //设置statebackend
        //env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9000/flink/checkpoints"));
        //env.setStateBackend(new Ro)


        //获取数据源
        DataStreamSource<Long> text1 = env.addSource(new MyParalleSource()).setParallelism(1);

        DataStreamSource<Long> text2 = env.addSource(new MyParalleSource()).setParallelism(1);

        SingleOutputStreamOperator<Object> text2_str= text2.map(new MapFunction<Long, Object>() {
            public String map(Long value) throws Exception {
                return "str_"+value;
            }
        });

        ConnectedStreams<Long, Object> connectStream = text1.connect(text2_str);

        SingleOutputStreamOperator<Object> result= connectStream.map(new CoMapFunction<Long, Object, Object>() {

            public Object map1(Long value) throws Exception {
                return value;
            }

            public Object map2(Object value) throws Exception {
                return value;
            }
        });


        //打印结果
        result.print().setParallelism(1);

        String jobName = StreamingDemoWithMyNoPralalleSource.class.getSimpleName();

        env.execute("aaa");
    }
}
