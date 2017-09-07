
//import com.redis.RedisClient

import Util.{KafkaConf, KafkaManager, RedisClient}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.Map
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool


object streamingStoreZK_3_PVUV {

  def processRdd(spark: SparkSession, rdd: RDD[(String, String)],redisNums:Int): Unit = {
    import spark.implicits._

    val data = rdd.map(_._2)
    var  redisNum =redisNums
    val result = data.filter(log => (!log.contains("AWS")) &(!log.contains("CLE"))
    ).map { line =>
      var vars = line.split("\\|_\\|")

      val date = vars(2).split("T")(0).split("-").mkString

      val urlString = vars(3)
      val dataMap: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
      for (data <- urlString.split("\\?")(1).split("&")) {
        val dataList = data.split("=")
        if (dataList.size == 2) {
          dataMap(dataList(0)) = dataList(1)
        } else if (dataList.size == 1) {
          dataMap(dataList(0)) = ""
        }
      }

      var ws = if (dataMap.contains("WS"))  dataMap("WS")  else "00000000"
      var cuc = if (dataMap.contains("CUC")) dataMap("CUC") else "11111111"
      (date, ws, cuc) //(ip ,user)
    }

    result.foreachPartition { partition =>
      val jedis = RedisClient.pool.getResource
      if (jedis!=null){
        redisNum = redisNum+1
        println("RedisClient.pool.getNumActive()"+RedisClient.pool.getNumActive())
        println("链接数为："+redisNum)
      }
      jedis.select(11)
      partition.foreach { line =>

        val date = line._1.toString()
        val url = line._2.toString()
        val cuc = line._3.toString()

        jedis.hincrBy("PV:" + date + ":" + url, url, 1)


        jedis.sadd("UV:" + date + ":" + url, url + cuc)

      }
      jedis.close()
    }
  }


  def main(args: Array[String]) {
    //.master("spark://10.250.100.17:7077")
    val spark = SparkSession.builder().appName("pvuv").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))
    import spark.implicits._
    var redisNums = 0
    val km = new KafkaManager(KafkaConf.kafkaParams)
    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, KafkaConf.kafkaParams, KafkaConf.topicsSet)

    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        //处理消息  
        processRdd(spark, rdd,redisNums)
        //更新offsets  
        km.updateZKOffsets(rdd)
      }
    })
    ssc.start()
    ssc.awaitTermination()

  }

}



//object RedisClient extends Serializable {
//  val redisHost = "10.250.100.20"
//  val redisPort = 6379
//  val redisTimeout = 30000
//  val auth = "lenovo-1234"
//  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout, auth)
//  lazy val hook = new Thread {
//    override def run = {
//      println("Execute hook thread: " + this)
//      pool.destroy()
//    }
//  }
//  sys.addShutdownHook(hook.run)
//}