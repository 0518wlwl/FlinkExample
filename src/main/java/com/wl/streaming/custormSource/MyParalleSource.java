package com.wl.streaming.custormSource;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/*
 *自定义一个并支持并行度的source
 */
public class MyParalleSource extends RichParallelSourceFunction<Long> {

    private long count = 0;

    private boolean isRunning = true;

    /*
     *主要的方法
     * 启动一个source
     * 大部分情况下，就需要在这个run方法中实现一个循环，这样就可以循环产生数据了
     */
    public void run(SourceContext<Long> ctx) throws Exception{
        while (isRunning){
            ctx.collect(count);
            count++;

            //每一秒产生一条数据
            Thread.sleep(1000);
        }
    }

    /*
     * 取消一个cancle的时候会调用的方法
     */
    public void cancel(){

        isRunning = false;
    }
}
