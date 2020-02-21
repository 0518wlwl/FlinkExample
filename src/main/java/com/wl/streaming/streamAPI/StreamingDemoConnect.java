package com.wl.streaming.streamAPI;

import com.wl.streaming.custormSource.MyParalleSource;
import com.wl.streaming.custormSource.StreamingDemoWithMyNoPralalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamingDemoConnect {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
