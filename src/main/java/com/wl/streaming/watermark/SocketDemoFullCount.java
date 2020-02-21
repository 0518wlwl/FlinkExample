package com.wl.streaming.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*
 * window 全量聚合
 */
public class SocketDemoFullCount {

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
        
        String hostname = "hadoop102";
        String delimiter = "\n";

        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();

        //连接socket 获取输入的数据
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> intData= text.map(new MapFunction<String, Tuple2<Integer,Integer>>() {
            public Tuple2<Integer,Integer> map(String value) throws Exception {
                return new Tuple2<Integer, Integer>(1,Integer.parseInt(value));
            }
        });

        intData.keyBy(0)
                .timeWindow(Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<Integer, Integer>, String, Tuple, TimeWindow>() {
                    public void process(Tuple key, Context context, Iterable<Tuple2<Integer, Integer>> elements, Collector<String> out) throws Exception {
                        long count = 0;
                        for (Tuple2<Integer,Integer> element:elements) {
                            count++;

                        }
                        out.collect("window:"+context.window()+",count:"+count);
                    }
                }).print();


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
