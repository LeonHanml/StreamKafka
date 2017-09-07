
//import com.redis.RedisClient

import Util.{KafkaConf, KafkaManager, PropUtil, RedisClient,tools}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * add time log
  */
object LenovoComPvuv2 {

  def main(args: Array[String]) {
    //.master("spark://10.250.100.17:7077")
    val spark = SparkSession.builder().appName(PropUtil.getProperty("appName")).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(PropUtil.getProperty("seconds").toInt))

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

    val result = data.filter((line => Filter(line))
    ).map { line =>
      var vars = line.split("\\|_\\|")
      //2017-07-27T02:57:00+08:00

      val datetime = vars(2).split("T")
      val date = datetime(0).split("-").mkString
      val time = datetime(1).split("+")(0).mkString
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


      var ws = if (dataMap.contains("WS")) dataMap("WS") else "00000000"
      var cuc = if (dataMap.contains("CUC")) dataMap("CUC") else "11111111"
      var rc = if(dataMap.contains("RC"))  dataMap("RC") else "1"
      var un =  if(dataMap.contains("UN"))  dataMap("UN") else ""
      (date, time, ws, cuc,rc,un) //(ip ,user)
    }

    result.foreachPartition { partition =>
      val jedis = RedisClient.pool.getResource

      jedis.select(14)
      partition.foreach { line =>

        val date = line._1.toString()
        val time = line._2.substring(0,5)
        val url = "lenovo.com"
        val cuc = line._4.toString()
        val rc = line._5.toInt
        val un = line._6.toString()

        jedis.hincrBy("RC:" + date + ":" + time + ":" + url, url, 1)
        jedis.hincrBy("PV:" + date + ":" + time + ":" + url, url, 1)


        jedis.sadd("UV:" + date + ":" + time + ":" + url, url + cuc)

      }
      jedis.close()
    }
  }


  def Filter(line: String): Boolean = {

    var Filter = false
    try {
      if (line.contains("WS=10000001&") && (!line.contains("CLE")) && (!line.toLowerCase().contains("spider")) && ifShopFlow(line)) {
        Filter = true
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    Filter

  }

  def ifShopFlow(line: String): Boolean = {
    var shopFlow = false
    try {
      val dataList: Array[String] = line.split("\\|_\\|")
      val timeString = dataList(2).split("\\+")(0).replace("T", " ")
      val currentDay = timeString.split("T")(0).replace("-", "")
      val urlString = dataList(3)
      var dataMap: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
      dataMap = tools.dataSplit(urlString)

      val pagesite = dataMap("PS")
      val pageurl = java.net.URLDecoder.decode(java.net.URLDecoder.decode(java.net.URLDecoder.decode(dataMap("PU"))))

      //println(s"pagesite: $pagesite, pageurl: $pageurl")

      val lenovoMallSite = List("www.lenovo.com.cn", "cart.lenovo.com.cn", "order.lenovo.com.cn", "i.lenovo.com.cn", "z.lenovo.com.cn", "s.lenovo.com.cn", "coupon.lenovo.com.cn", "m.lenovo.com.cn", "m.cart.lenovo.com.cn", "3g.lenovo.com.cn", "app_host_name", "m.order.lenovo.com.cn", "m.coupon.lenovo.com.cn", "buy.lenovo.com.cn", "mbuy.lenovo.com.cn", "cashier.lenovo.com.cn")
      val thinkMallSite = List("www.thinkworldshop.com.cn", "cart.thinkworldshop.com.cn", "thinkpad.lenovo.com.cn", "order.thinkworldshop.com.cn", "i.thinkworldshop.com.cn", "mobile.thinkworldshop.com.cn", "m.cart.thinkworldshop.com.cn", "s.thinkworldshop.com.cn", "coupon.thinkworld.com.cn", "m.coupon.thinkworldshop.com.cn", "m.order.thinkworldshop.com.cn")

      //联想商城
      if (lenovoMallSite.contains(pagesite)
        || (pagesite.equals("pay.i.lenovo.com") && (pageurl.contains("plat=4") || pageurl.contains("b2cPc_url") || pageurl.contains("plat=1") || pageurl.contains("b2cWap_url") || pageurl.contains("plat=3")))
        || (pagesite.equals("c.lenovo.com.cn") && pageurl.contains("lenovo"))
        || (pageurl.toLowerCase().equals("shopid=1"))) {
        shopFlow = true

      }
      //Think商城
      if (thinkMallSite.contains(pagesite)
        || (pagesite.equals("pay.i.lenovo.com") && (pageurl.contains("plat=5") || pageurl.contains("tkPc_url") || pageurl.contains("plat=8") || pageurl.contains("tkWap_url")))
        || (pagesite.equals("c.lenovo.com.cn") && pageurl.contains("think"))
        || (pageurl.toLowerCase().equals("shopid=2"))) {
        shopFlow = true
      }

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    shopFlow
  }

}


