package com.wl.batch.batchAPI

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer

/**
 * 累加器
 */
object BatchDemoCounterScala {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

     val text=env.fromElements("a","b","c")

     val res=text.map(new RichMapFunction[String,String] {
      //1.定义累加器
      val numLines= new IntCounter


      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        //2.注册累加器
        getRuntimeContext.addAccumulator("num-lines",this.numLines)

      }

      override def map(value: String): String ={
        this.numLines.add(1)

        value
      }
    })

    res.writeAsText("d:\\data\\count2")

    val jobResult = env.execute("BatchDemoCounterScala")

    val num= jobResult.getAccumulatorResult[Int]("num-lines")

    println("num:"+num)
  }

}
