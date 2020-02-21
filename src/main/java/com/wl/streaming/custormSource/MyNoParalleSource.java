package com.wl.streaming.custormSource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/*
 *自定义实现并行度为1的source
 *
 * 模拟产生从1开始的递增数字
 *
 * 注意 SourceFunction 和  SourceContext都需要指定泛型， 要不报错
 * Caused by: org.apache.flink.api.common.functions.InvalidTypesException:
 *  The types of the interface org.apache.flink.streaming.api.functions.source.SourceFunction could not be inferred.
 * Support for synthetic interfaces, lambdas, and generic or raw types is limited at this point
 */
public class MyNoParalleSource implements SourceFunction<Long> {

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
