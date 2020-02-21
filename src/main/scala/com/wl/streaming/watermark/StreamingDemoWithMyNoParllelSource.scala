package com.wl.streaming.watermark

import com.wl.streaming.MyNoParllelSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingDemoWithMyNoParllelSource {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._
    val text= env.addSource(new MyNoParllelSource).setBufferTimeout(2)

    val mapData= text.map(line =>{
      println("接收到的数据:"+line)
      line
    })

    val sum=mapData.timeWindowAll(Time.seconds(2)).sum(0)


    sum.print().setParallelism(1)

    env.execute("StreamingDemoWithMyNoParllelSource")
  }
}
