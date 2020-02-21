package com.wl.batch.batchAPI

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object BatchDemoDistinctScala {

  def main(args: Array[String]): Unit = {
    val env= ExecutionEnvironment.getExecutionEnvironment

    val data = ListBuffer[String]()

    data.append("hello you")
    data.append("hello me")

 
    import org.apache.flink.api.scala._
    val text= env.fromCollection(data)

    val flagMapData= text.flatMap(line =>{
     val words=  line.split("\\W+")
      for(word <- words){
        println("单词:"+word)
      }
      words
    })

    flagMapData.distinct().print();
  }

}
