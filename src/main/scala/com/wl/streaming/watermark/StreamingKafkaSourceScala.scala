package com.wl.streaming.watermark

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/*
 *滑动窗口  每隔1秒统计最近两秒内的数据 打印到控制台
 */
object StreamingKafkaSourceScala {

  def main(args: Array[String]): Unit = {
    val env= StreamExecutionEnvironment.getExecutionEnvironment

    //隐士转换
    import  org.apache.flink.api.scala._

    val topic = "t1";
    val prop = new Properties();
    prop.setProperty("bootstrap.servers", "hadoop102:9092")
    prop.setProperty("group.id", "con1")


    val myConsumer=new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),prop)

    val text = env.addSource(myConsumer)

    //每隔1000 ms进行启动一个检查点，【设置checkpoint的周期】
    env.enableCheckpointing(1000)
    //高级选项
    //设置模式exactly-once（这是默认值）
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //确保检查检查点之间至少500ms的间隔 【checkpoint的最小时间】
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    //检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
    env.getCheckpointConfig.setCheckpointTimeout(6000)
    //同一时间只允许一个检查点
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //表示一旦Flink处理程序被cancel后，会保留checkpoint的数据，以便根据需求恢复到指定的check
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    text.print()

    env.execute("StreamingKafkaSourceScala")
  }
}
