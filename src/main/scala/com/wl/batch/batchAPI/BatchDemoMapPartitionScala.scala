package com.wl.batch.batchAPI


import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object BatchDemoMapPartitionScala {

  def main(args: Array[String]): Unit = {
    val env= ExecutionEnvironment.getExecutionEnvironment

    val data = ListBuffer[String]()

    data.append("hello you")
    data.append("hello me")


    import org.apache.flink.api.scala._
    val text= env.fromCollection(data)

    text.mapPartition(it =>{
      val res = ListBuffer[String]()
      while (it.hasNext){
        val line = it.next()
        val words = line.split("\\W+")
        for(word <- words){
          res.append(word)
        }
      }
      res
    }).print()
  }

}
