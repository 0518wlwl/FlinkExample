package com.wl.streaming.watermark

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/*
 *滑动窗口  每隔1秒统计最近两秒内的数据 打印到控制台
 */
object SocketWindowWordCountScala {

  def main(args: Array[String]): Unit = {

    //获取socket端口号
    val port : Int=try{
      ParameterTool.fromArgs(args).getInt("port")
    }catch {
      case e:Exception => {
        System.err.println("No port set use default port 9002")
      }
        9002
    }

    //获取运行环境
    val env:StreamExecutionEnvironment= StreamExecutionEnvironment.getExecutionEnvironment
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

    //设置statebackend
    //env.setStateBackend(new MemoryStateBackend());
    env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9000/flink/checkpoints"))
    //env.setStateBackend(new Ro)

    //链接socket获取输入数据
    val text=env.socketTextStream("hadoop102",port,'\n')

    //隐式转换
    import org.apache.flink.api.scala._
    //解析数据（把数据打平） 分组 窗口 计算  聚合
    val winwosCounts= text.flatMap(line =>line.split("\\s"))//打平  把每一行单词都切开
      .map(w=>WordWithCount(w,1)) //转换为 word 1 这种形式
      .keyBy("word") //分组
      .timeWindow(Time.seconds(2),Time.seconds(1))//指定窗口大小 时间间隔
      .sum("count")//
      //.reduce((a,b)=>WordWithCount(a.word,a.count+b.count))

    //打印到控制台
    winwosCounts.print().setParallelism(1);

    env.execute("Socket window count")



  }

  case class WordWithCount(word:String,count:Long)
}
