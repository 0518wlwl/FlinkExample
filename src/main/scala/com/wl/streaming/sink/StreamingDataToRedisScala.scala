package com.wl.streaming.sink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


/*
 *滑动窗口  每隔1秒统计最近两秒内的数据 打印到控制台
 */
object StreamingDataToRedisScala {

  def main(args: Array[String]): Unit = {

    //获取socket端口号
     val port=9002

    //获取运行环境
    val env:StreamExecutionEnvironment= StreamExecutionEnvironment.getExecutionEnvironment

    //链接socket获取输入数据
    val text=env.socketTextStream("hadoop102",port,'\n')

    //隐式转换
    import org.apache.flink.api.scala._

    val l_wordsData= text.map(line =>("l_words_scala",line))

    val conf= new FlinkJedisPoolConfig.Builder().setHost("192.168.64.154").setPort(6379).build()

   val redisSink=  new RedisSink[Tuple2[String,String]](conf,new MyRedisMapper)


    l_wordsData.addSink(redisSink)

    env.execute("Socket window count")



  }

  class MyRedisMapper extends RedisMapper[Tuple2[String,String]]{
    override def getCommandDescription: RedisCommandDescription ={
      new RedisCommandDescription(RedisCommand.LPUSH)
    }

    override def getKeyFromData(data: (String, String)): String ={
      data._1
    }

    override def getValueFromData(data: (String, String)): String ={
      data._2
    }
  }
}

