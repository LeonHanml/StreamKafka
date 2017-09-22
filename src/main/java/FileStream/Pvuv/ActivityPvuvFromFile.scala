package FileStream.Pvuv

//import com.redis.RedisClient

import FileStream.UtilFromConfiguration.{CommonPvuv, PropUtil, RedisClient, Tools}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map

/**
  * 从File中读取并计算数据
  */
object ActivityPvuvFromFile {
  //  val log = Logger.getLogger(refitPvuv.getClass.getName)
  //  log.setLevel(Level.ALL)
  //  log.addAppender(new ConsoleAppender(new SimpleLayout(), "System.out"))

  def main(args: Array[String]) {

    refit(PropUtil.getProperty("datefit"),PropUtil.getProperty("filePath"))
  }

  def refit(date: String,filePath0:String): Unit = {
    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[CommonPvuv]))
    val spark = SparkSession.builder().appName("LenovoPvuv0").config(conf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext


    val year = date.substring(0, 4)
    val month = date.substring(4, 6)
    val day = date.substring(6, 8)
    var filePath = filePath0
    if(filePath.equals("")){
      filePath = s"hdfs://10.250.100.47:8020/flume/$year/$month/$day/*/traffic*"
    }

//    val file = s"hdfs://10.250.100.47:8020/flume/2017/07/25/*/traffic*"
    //  val rdd = sc.textFile("hdfs://10.250.100.47:8020/flume/2017/08/01/11/traffic*")
    val rdd: RDD[String] = sc.textFile(filePath)

    processRdd(rdd, date, sc)

  }


  def processRdd(rdd: RDD[(String)], dateRefit: String, sc: SparkContext): Unit = {

    //    val resultUvMap: Map[String, Int] = Map()
    //    val resultRcMap: Map[String, Int] = Map()
    //    val resultUnMap: Map[String, Int] = Map()
    val data = rdd
    //    log.warn("开始计算result")

    println("开始计算result")

    val result = data.filter(line => (!line.toLowerCase().contains("spider"))).map(line => Tools.dataSplitFromLine(line))
      .filter(dataMap => Tools.isShopFlow(dataMap) && dataMap("WS").equals("10000001") && (!dataMap.contains("CLE")))

    var resultPvMap: Map[String, Int] = Map()
    val sm = scala.collection.immutable.SortedMap
    val pv = result.map { dataMap =>

      val date = dataMap("date")
      val time = dataMap("time").substring(0, 5) + ":00"
      var ws = if (dataMap.contains("WS")) dataMap("WS") else "WS-NULL"
      val url = ws

//      var cuc = if (dataMap.contains("CUC")) dataMap("CUC") else "CUC-NULL"
//      var rcStr = if (dataMap.contains("RC")) dataMap("RC") else "RC-NULL"
//      var unStr = if (dataMap.contains("UN")) dataMap("UN") else "UN-NULL"
      var puStr = if (dataMap.contains("PU")) Tools.urlDecoder(dataMap("PU")) else "PU-NULL"
      var ttStr = if (dataMap.contains("TT")) Tools.urlDecoder(dataMap("TT")) else "TT-NULL"
      var key = ""
      var value = 0
      if (puStr.contains("/activity/")) {

        val tt = "联想商城粉丝节大促，爆款直降还有满千减百，大促狂欢秒不停，快来分享吧！"
        if(tt.equals(ttStr)){
          key = time.toString
          value = 1

        }

      }

      //      val key = s"$date|$time|$url"

      (key,value)
    }.reduceByKey(_ + _)


    val jedis = RedisClient.pool.getResource
    val redisdb = PropUtil.getProperty("redis.db").toInt
    println(s"redisddb:$redisdb")
    jedis.select(redisdb)
    var incre = 0
    val sortArray = pv.collect().sortBy(arr => arr._1).foreach {

      arr =>
        if (!(arr._1.equals(""))){
          incre += arr._2
          println(arr._1 + "--" + incre)
          jedis.lpush("activity_pv",arr._1 + "--" + incre)
        }

    }




    jedis.close()

  }
}


