package com.wl.streaming.watermark;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/*
*滑动窗口计算
*
* 通过socket模拟生产单词数据
* flink对数据进行统计计算
*
* 需要数显每隔1秒对最近两秒的数据进行汇总计算
*/
public class SocketWindowWordCountJava {

    public static void main(String[] args) throws Exception{

        //获取需要的端口号
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        }catch (Exception e){
            System.err.println("No port set ,use default port 9001");
            port = 9001;
        }

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
        env.setStateBackend(new FsStateBackend("hdfs://192.168.64.145:9000/flink/checkpoints"));
        //env.setStateBackend(new Ro)



        String hostname = "hadoop102";
        String delimiter = "\n";

        //连接socket 获取输入的数据
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);

        DataStream<WordWithCount> widowCounts= text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String[] splits = value.split("\\s");
                for (String word:splits){
                    out.collect(new WordWithCount(word,1L));
                }

            }
        }).keyBy("word")
                .timeWindow(Time.seconds(2),Time.seconds(1))//指定时间窗口大小为2秒，执行时间间隔为1秒
        .sum("count");//在这里reduce或者sum都可以
      //把数据打印到控制台并设置并行度
        widowCounts.print().setParallelism(1);

        //这一行代码一定要实现。否则程序不执行
        env.execute("Socket window count");
    }

    public static class WordWithCount{
        public String word;
        public long count;
        public WordWithCount(String word,Long count){
            this.word = word;
            this.count = count;
        }
        public WordWithCount(){

        }

        @Override
        public String toString() {
            return word+":"+count;
        }
    }
}
