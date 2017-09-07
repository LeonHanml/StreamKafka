
//import com.redis.RedisClient

import java.text.SimpleDateFormat

import Model.CommonPvuv
import Util.{KafkaConf, KafkaManager, MySqlPool, RedisClient, tools}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

//activity 抽出方法

/***
  * 只计算 activity 活动 的pvuv计算
  * 没有计入的活动 将存到  jedis.sadd(s"$date:ttset", ttStr) 集合中
  */
object LenovoComPvuv9 {
//@transent
  def main(args: Array[String]) {
    //.master("spark://10.250.100.17:7077")
    val conf =new  SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[CommonPvuv]))

    val spark = SparkSession.builder().appName("Activity9_Lenovo.com").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").config(conf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(10))

    val km = new KafkaManager(KafkaConf.kafkaParams)
    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, KafkaConf.kafkaParams, KafkaConf.topicsSet)

    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        //处理消息
        processRdd(spark, rdd)
        //更新offsets
        km.updateZKOffsets(rdd)
      }
    })
    ssc.start()
    ssc.awaitTermination()

  }

  def processRdd(spark: SparkSession, rdd: RDD[(String, String)]): Unit = {

    val data = rdd.map(_._2)
//    val ttMap = Map(("tt1", "联想商城周年庆，购爆款， GO狂欢，更有秒杀低至1折起，赶快来分享！"), ("tt2", "联想平板电脑TAB4/TAB4 Plus新品发布"))

//    val ttMap = Map(("联想商城周年庆，购爆款， GO狂欢，更有秒杀低至1折起，赶快来分享！", "tt1"), ("联想平板电脑TAB4/TAB4 Plus新品发布", "tt2"))
    val result = data.filter(line => (!line.toLowerCase().contains("spider"))).map(line => tools.dataSplitFromLine(line))
      .filter(dataMap => tools.isShopFlow(dataMap) && dataMap("WS").equals("10000001") && (!dataMap.contains("CLE")))


    result.foreachPartition { partition =>

      val jedis = RedisClient.pool.getResource
      jedis.select(14)
      val stateKey = "shopflowUvByHourState"

      val previous = "previous"
      val conn = MySqlPool.getJdbcConn()
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")

      partition.foreach { dataMap =>
        try {
          val date = dataMap("date")
          val time = dataMap("time")
          val stmt = conn.createStatement()
//          println("开始创建类CommonPvuv")
          val cpvuv = new CommonPvuv(dataMap ,jedis,conn )
//          cpvuv.setCommonPvuv()
          cpvuv.setActivityPvuv()

          val minTSlogTimeMillions = tools.dateToStamp(s"$date $time").toLong
          val insertKey = "State_" + date

          if (jedis.exists(stateKey)) {
            val state = jedis.get(stateKey).toLong

            if (minTSlogTimeMillions > state) {
//              println("开始读取数据到MySQL")
//              cpvuv.getCommonPvuv()
              cpvuv.getActivityPvuv()

              jedis.set(stateKey, minTSlogTimeMillions.toString())
              jedis.lpush(insertKey, "1")
            }

          } else {
            jedis.set(stateKey, minTSlogTimeMillions.toString())
            jedis.lpush(insertKey, "1")
          }
        }
        catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
      MySqlPool.releaseConn(conn)
      jedis.close()
    }
  }


}
