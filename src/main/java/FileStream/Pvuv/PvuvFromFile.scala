package FileStream.Pvuv

//import com.redis.RedisClient

import FileStream.UtilFromConfiguration.{CommonPvuv, PropUtil, RedisClient, Tools}
import kafka.serializer.StringDecoder
import org.apache.log4j.{ConsoleAppender, Level, Logger, SimpleLayout}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.Map

/**
  * 从File中读取并计算数据
  * 在配置文件 pvuv.properties中
  * datefit表示要计算的日期，例如：201708205
  * filePath 表示要读取日志的路径
  * 数据会写入到redis中
  */
object PvuvFromFile {

  def main(args: Array[String]) {

    refit(PropUtil.getProperty("datefit"),PropUtil.getProperty("filePath"))
  }

  def refit(date: String,filePath0:String): Unit = {
    val conf =new  SparkConf()
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

//    val filePath = s"hdfs://10.250.100.47:8020/flume/$year/$month/$day/*/traffic*"
//    val file = s"hdfs://10.250.100.47:8020/flume/2017/07/25/*/traffic*"
    //  val rdd = sc.textFile("hdfs://10.250.100.47:8020/flume/2017/08/01/11/traffic*")
    val rdd: RDD[String] = sc.textFile(filePath,1)

    processRdd(rdd, date,sc)

  }


  def processRdd(rdd: RDD[(String)], dateRefit: String,sc:SparkContext): Unit = {

    val data = rdd
    //    log.warn("开始计算result")

    println("开始计算result")

    val result = data.filter(line => (!line.toLowerCase().contains("spider"))).map(line => Tools.dataSplitFromLine(line))
      .filter(dataMap => Tools.isShopFlow(dataMap) && dataMap("WS").equals("10000001") && (!dataMap.contains("CLE")))

    var resultPvMap: Map[String, Int] = Map()
    val sm =scala.collection.immutable.SortedMap
    val pv = result.map { dataMap =>

      val date = dataMap("date")
      val time = dataMap("time").substring(0, 5) + ":00"
      var ws = if (dataMap.contains("WS")) dataMap("WS") else "WS-NULL"
      val url = ws

      var cuc = if (dataMap.contains("CUC")) dataMap("CUC") else "CUC-NULL"
      var rcStr = if (dataMap.contains("RC")) dataMap("RC") else "RC-NULL"
      var unStr = if (dataMap.contains("UN")) dataMap("UN") else "UN-NULL"

//      val key = s"$date|$time|$url"
      val key = s"$time"
      (key, 1)
    }.reduceByKey(_ + _)


    val jedis = RedisClient.pool.getResource
    val redisdb = PropUtil.getProperty("redis.db").toInt
    println(s"redisddb:$redisdb")
    jedis.select(redisdb)
    var incre = 0

    /**
      * Collection 中都自带有排序函数
      */
    val sortArray = pv.collect().sortBy(arr=>arr._1).foreach {

      arr=>

        incre+=arr._2
        println(arr._1+"--"+incre)
        jedis.lpush("pv",arr._1 + "--" + incre)
    }









//    val uv = result.map { dataMap =>
//
//      val date = dataMap("date")
//      val time = dataMap("time").substring(0, 5) + ":00"
//      var ws = if (dataMap.contains("WS")) dataMap("WS") else "WS-NULL"
//      val url = ws
//
//      var cuc = if (dataMap.contains("CUC")) dataMap("CUC") else "CUC-NULL"
//      var rcStr = if (dataMap.contains("RC")) dataMap("RC") else "RC-NULL"
//      var unStr = if (dataMap.contains("UN")) dataMap("UN") else "UN-NULL"
//
//      val key = s"$date:$time:$url"
//      val unique = url + cuc
//      (key, cuc)
//
//    }.map(x => (x, 1)).reduceByKey(_ + _).foreach { line =>
//      resultUvMap(line._1._1) = line._2
//    }
//
//    val rc = result.filter(dataMap => dataMap.contains("RC") && dataMap("RC").equals("0")).
//      map { dataMap =>
//        val date = dataMap("date")
//        val time = dataMap("time").substring(0, 5) + ":00"
//        var ws = if (dataMap.contains("WS")) dataMap("WS") else "WS-NULL"
//        val url = ws
//
//        var cuc = if (dataMap.contains("CUC")) dataMap("CUC") else "CUC-NULL"
//        var rcStr = if (dataMap.contains("RC")) dataMap("RC") else "RC-NULL"
//        var unStr = if (dataMap.contains("UN")) dataMap("UN") else "UN-NULL"
//
//        val key = s"$date:$time:$url"
//        val unique = url + cuc
//        (key, cuc)
//
//      }.map(x => (x, 1)).reduceByKey(_ + _).foreach { line =>
//      resultUvMap(line._1._1) = line._2
//    }
//
//    val un = result.filter(dataMap => dataMap.contains("UN") && dataMap("UN").equals("")).
//      map { dataMap =>
//        val date = dataMap("date")
//        val time = dataMap("time").substring(0, 5) + ":00"
//        var ws = if (dataMap.contains("WS")) dataMap("WS") else "WS-NULL"
//        val url = ws
//
//        var cuc = if (dataMap.contains("CUC")) dataMap("CUC") else "CUC-NULL"
//        var rcStr = if (dataMap.contains("RC")) dataMap("RC") else "RC-NULL"
//        var unStr = if (dataMap.contains("UN")) dataMap("UN") else "UN-NULL"
//
//        val key = s"$date:$time:$url"
//        val unique = url + cuc
//        (key, cuc)
//
//      }.map(x => (x, 1)).reduceByKey(_ + _).foreach { line =>
//      resultUvMap(line._1._1) = line._2
//    }
//
//    //    val conn = MySqlPool.getJdbcConn()


//    for (time <- resultPvMap.keySet) {
//            val sql = "update shopflow_total_pvuv " +
//              " set pv = " + resultPvMap(time) +
//              ", uv = " + resultUvMap(time) +
//              ", rc = " + resultRcMap(time) +
//              ", un = " + resultUnMap(time) +
//              " where date = \"" + dateRefit + "\" and insert_time = \"" + time + "\""
//      jedis.set(time.toString, resultPvMap(time).toString)
//    }




  jedis.close()

  }
}


//object LogPvUv1 extends Serializable {

//  def run(date: String): Unit = {
//    //    var str = Tool.dateToStamp("2012-02-22 12:23:34")
//     val rdd = sc.textFile("hdfs://10.250.100.47:8020/flume/2017/08/01/11/traffic*")
//    // date=20170811
//    val year = date.substring(0, 4)
//    val month = date.substring(4, 6)
//    val day = date.substring(6, 8)
//    val filePath = "hdfs://10.250.100.47:8020/flume/" + year + "/" + month + "/" + day + "/*/traffic*"
//    // val file = "hdfs://10.250.100.47:8020/flume/2017/07/25/*/traffic*"
//
//    val rdd = sc.textFile(filePath)
//    // for(i <- Range(0,2)){
//    //      println(rdd.)
//    // }
//    // println(rdd.count())//4136672
//    val result = rdd.filter(line => (!line.toLowerCase().contains("spider"))).map(line => tools.dataSplitFromLine(line)).filter(dataMap => tools.isShopFlow(dataMap) && dataMap("WS").equals("10000001") && (!dataMap.contains("CLE")))
//    println(result.count())
//  }
//}