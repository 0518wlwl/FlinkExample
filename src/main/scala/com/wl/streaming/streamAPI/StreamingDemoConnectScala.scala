package com.wl.streaming.streamAPI

import com.wl.streaming.MyNoParllelSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingDemoWithMyNoParllelSource {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._

    val text1= env.addSource(new MyNoParllelSource)
    val text2= env.addSource(new MyNoParllelSource)

    val text2_str = text2.map("str"+_)

    val connectedStreams= text1.connect(text2_str)

    val result= connectedStreams.map(line1=>{line1},line2=>{line2})

    result.print().setParallelism(1)


    env.execute("StreamingDemoWithMyNoParllelSource")
  }
}
