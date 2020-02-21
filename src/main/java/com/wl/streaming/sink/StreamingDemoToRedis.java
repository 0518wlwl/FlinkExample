package com.wl.streaming.sink;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/*
 * 接受 socket数据，把数据保存到redis中
 * list
 *
 * lpush list_key value
 */
public class StreamingDemoToRedis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("192.168.64.145", 9001, "\n");

        //lpush l_words word
        //对数据进行组装

        DataStream l_wordsData = text.map(new MapFunction<String, Tuple2<String, String>>() {
            public Tuple2<String,String> map(String value) throws Exception {
                return new Tuple2("1_words", value);
            }
        });

        //创建redis配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.64.154").setPort(6379).build();
       //创建redissink
        RedisSink<Tuple2<String,String>> redisSink = new RedisSink(conf, new MyRedisMapper());

        l_wordsData.addSink(redisSink);

        env.execute("StreamingDemoToRedis");
    }
    public static class MyRedisMapper implements RedisMapper<Tuple2<String,String>> {

        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }

        public String getKeyFromData(Tuple2<String, String> date) {
            return date.f0;
        }

        public String getValueFromData(Tuple2<String, String> date) {
            return date.f1;
        }
    }
}
