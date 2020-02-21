package com.wl.streaming.streamAPI;

import com.wl.streaming.custormSource.MyParalleSource;
import com.wl.streaming.custormSource.StreamingDemoWithMyNoPralalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;

/*
  * 可能在实际工作中，源数据流中混合了多种类似的数据，多种类型的数据处理规则不一样，所有就可以在根据一定的规则
  * 把一个数据流切分多个数据流，这样每个数据流就可以使用不同的处理逻辑了
  */
public class StreamingDemoSplit {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        DataStreamSource<Long> text= env.addSource(new MyParalleSource()).setParallelism(1);


        //对流进行切分，按照数据的奇偶性进行区分
         SplitStream<Long> splitStream= text.split(new OutputSelector<Long>() {
             public Iterable<String> select(Long value) {

                 ArrayList<String> outPut = new ArrayList<String>();

                 if(value %2 ==0){
                     outPut.add("even");//偶数
                 }else {
                     outPut.add("odd");//奇数
                 }
                 return outPut;
             }
         });

         //选择一个或者多个切分后的流
        DataStream<Long> even = splitStream.select("even");

        //打印结果
        even.print().setParallelism(1);

        String jobName = StreamingDemoWithMyNoPralalleSource.class.getSimpleName();

        env.execute("aaa");
    }
}
