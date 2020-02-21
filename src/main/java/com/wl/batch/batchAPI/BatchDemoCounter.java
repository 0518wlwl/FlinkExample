package com.wl.batch.batchAPI;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;


/**
 *全局的
 * counter 计算器
 *
 * 需求:
 * 计算map函数中处理了多少数据
 *
 * 注意:只有在任务结束以后才能获取到累加器的值
 */
public class BatchDemoCounter {

    public static void main(String[] args) throws Exception {

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> text = env.fromElements("a", "b", "c");

        MapOperator<String, String> result = text.map(new RichMapFunction<String, String>() {
           //1.创建一个累加器
            private IntCounter numLines= new IntCounter();
            @Override
            public void open(Configuration parameters) throws Exception {

                //2.注册一个累加器
                getRuntimeContext().addAccumulator("num-lines",this.numLines);
                super.open(parameters);
            }

            int sum = 0;

            public String map(String value) throws Exception {
                this.numLines.add(1);
                return value;
            }
        }).setParallelism(4);

       // result.print();

        result.writeAsText("D:\\date\\count");

        JobExecutionResult jobresult= env.execute("counter");
        //3.获取累加器
        int num = jobresult.getAccumulatorResult("num-lines");

        System.out.println("num:"+num);
    }

}
