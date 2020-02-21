package com.wl.streaming

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/*
 *创建自定义并行度为1的source
 * 实现从1开始产生的递增数字
 *
 */
class MyRichParllelSource extends RichParallelSourceFunction[Long]{

  var count = 1L;
  var isRunning = true;

  override def run(ctx:SourceContext[Long])={
     while (isRunning){
       ctx.collect(count)
       count+=1
       Thread.sleep(1000)
     }
  }

  override def cancel()={
    isRunning = false
  }

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def clone(): AnyRef = super.clone()
}
