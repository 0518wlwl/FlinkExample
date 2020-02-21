package com.wl.batch.batchAPI

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object BatchDemoDisCacheScala {

  def main(args: Array[String]): Unit = {
    val env= ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    //1.注册临时文件
    env.registerCachedFile("D:\\date\\file\\a.txt","a.txt")

    val data= env.fromElements("a","b","c","d")

   val result= data.map(new RichMapFunction[String,String] {

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)

        val myFile= getRuntimeContext.getDistributedCache.getFile("a.txt")
        val Lines= FileUtils.readLines(myFile)
        val it= Lines.iterator()
        while(it.hasNext){
          val line = it.next()
          println("line:"+line)
        }
      }

      override def map(value: String): String = {

        value
      }
    })

    result.print()

  }

}
