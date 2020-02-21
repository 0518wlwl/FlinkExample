package com.wl.streaming.streamAPI;


import com.wl.streaming.custormSource.MyNoParalleSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/*
 *使用并行度为 1 的source
 *
 */
public class StreamingDemoFilter {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        DataStreamSource text = env.addSource(new MyNoParalleSource());

       DataStream<Long> num= text.map(new MapFunction<Long,Long>() {
            public Long map(Long value) throws Exception {

                System.out.println("接收到数据:"+value);
                return value;
            }
        });

        DataStream<Long> resultData= num.filter(new FilterFunction<Long>() {
            public boolean filter(Long value) throws Exception {
                return value%2==0;
            }
        });


        //每两秒钟 处理一次数据
        DataStream<Long> sum = resultData.timeWindowAll(Time.seconds(2)).sum(0);

        //打印结果
        sum.print().setParallelism(1);

        String jobName = StreamingDemoFilter.class.getSimpleName();

        env.execute("aaa");
    }
}
