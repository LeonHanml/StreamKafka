
//import com.redis.RedisClient

import Util.{RedisClient, tools}
import kafka.serializer.StringDecoder
import org.apache.log4j.{ConsoleAppender, Level, Logger, SimpleLayout}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable.Map

/**
  * add rc  un
  */
object refitPvuv {
  val log = Logger.getLogger(refitPvuv.getClass.getName)
  log.setLevel(Level.ALL)
  log.addAppender(new ConsoleAppender(new SimpleLayout(), "System.out"))

  def main(args: Array[String]) {
    //.master("spark://10.250.100.17:7077")
    println("开始跑步-----------------------")
    refit("20170725")


  }

  def refit(date: String): Unit = {
    val spark = SparkSession.builder().appName("1000001_Lenovo.comPvuv_Refit").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    //  val rdd = sc.textFile("hdfs://10.250.100.47:8020/flume/2017/08/01/11/traffic*")

    val year = date.substring(0, 4)
    val month = date.substring(4, 6)
    val day = date.substring(6, 8)
    val filePath = "hdfs://10.250.100.47:8020/flume/" + year + "/" + month + "/" + day + "/*/traffic*"
    val file = "hdfs://10.250.100.47:8020/flume/2017/07/25/*/traffic*"
    val rdd = sc.textFile(filePath)


    processRdd(rdd, date)

  }


  def processRdd(rdd: RDD[(String)], dateRefit: String): Unit = {
    val resultPvMap: Map[String, Int] = Map()
    val resultUvMap: Map[String, Int] = Map()
    val resultRcMap: Map[String, Int] = Map()
    val resultUnMap: Map[String, Int] = Map()
    val data = rdd
    log.warn("开始计算result")
    println("开始计算result")
    var result = data.map(line => tools.dataSplitFromLine(line)).filter(dataMap => tools.isShopFlow(dataMap))


    result = data.filter(line => (!line.toLowerCase().contains("spider"))).map(line => tools.dataSplitFromLine(line))
      .filter(dataMap => tools.isShopFlow(dataMap) && dataMap("WS").equals("10000001") && (!dataMap.contains("CLE")))


    val pv = result.map { dataMap =>

      val date = dataMap("date")
      val time = dataMap("time").substring(0, 5) + ":00"
      var ws = if (dataMap.contains("WS")) dataMap("WS") else "WS-NULL"
      val url = ws

      var cuc = if (dataMap.contains("CUC")) dataMap("CUC") else "CUC-NULL"
      var rcStr = if (dataMap.contains("RC")) dataMap("RC") else "RC-NULL"
      var unStr = if (dataMap.contains("UN")) dataMap("UN") else "UN-NULL"

      val key = s"$date:$time:$url"
      (key, 1)
    }.reduceByKey(_ + _).foreach {
      line =>
        resultPvMap(line._1) = line._2
    }


    val uv = result.map { dataMap =>

      val date = dataMap("date")
      val time = dataMap("time").substring(0, 5) + ":00"
      var ws = if (dataMap.contains("WS")) dataMap("WS") else "WS-NULL"
      val url = ws

      var cuc = if (dataMap.contains("CUC")) dataMap("CUC") else "CUC-NULL"
      var rcStr = if (dataMap.contains("RC")) dataMap("RC") else "RC-NULL"
      var unStr = if (dataMap.contains("UN")) dataMap("UN") else "UN-NULL"

      val key = s"$date:$time:$url"
      val unique = url + cuc
      (key, cuc)

    }.map(x => (x, 1)).reduceByKey(_ + _).foreach { line =>
      resultUvMap(line._1._1) = line._2
    }

    val rc = result.filter(dataMap => dataMap.contains("RC") && dataMap("RC") .equals("0")).
      map { dataMap =>
        val date = dataMap("date")
        val time = dataMap("time").substring(0, 5) + ":00"
        var ws = if (dataMap.contains("WS")) dataMap("WS") else "WS-NULL"
        val url = ws

        var cuc = if (dataMap.contains("CUC")) dataMap("CUC") else "CUC-NULL"
        var rcStr = if (dataMap.contains("RC")) dataMap("RC") else "RC-NULL"
        var unStr = if (dataMap.contains("UN")) dataMap("UN") else "UN-NULL"

        val key = s"$date:$time:$url"
        val unique = url + cuc
        (key, cuc)

      }.map(x => (x, 1)).reduceByKey(_ + _).foreach { line =>
      resultUvMap(line._1._1) = line._2
    }
    val un =  result.filter(dataMap => dataMap.contains("UN") && dataMap("UN") .equals("")).
      map { dataMap =>
        val date = dataMap("date")
        val time = dataMap("time").substring(0, 5) + ":00"
        var ws = if (dataMap.contains("WS")) dataMap("WS") else "WS-NULL"
        val url = ws

        var cuc = if (dataMap.contains("CUC")) dataMap("CUC") else "CUC-NULL"
        var rcStr = if (dataMap.contains("RC")) dataMap("RC") else "RC-NULL"
        var unStr = if (dataMap.contains("UN")) dataMap("UN") else "UN-NULL"

        val key = s"$date:$time:$url"
        val unique = url + cuc
        (key, cuc)

      }.map(x => (x, 1)).reduceByKey(_ + _).foreach { line =>
      resultUvMap(line._1._1) = line._2
    }
    log.warn("result count:" + result.count())


    log.warn("resultPvMap count:" + resultPvMap.size)






    //    log.warn("resultPvMap count:"+resultUvMap.size)
    //    log.warn("resultRcMap count:"+resultRcMap.size)

    //    log.warn("resultUnMap count:"+resultUnMap.size)
    //    val conn = MySqlPool.getJdbcConn()
    val jedis = RedisClient.pool.getResource
    jedis.select(14)
    for (time <- resultPvMap.keySet) {


      val sql = "update shopflow_total_pvuv " +
        " set pv = " + resultPvMap(time) +
        ", uv = " + resultUvMap(time) +
        ", rc = " + resultRcMap(time) +
        ", un = " + resultUnMap(time) +
        " where date = \"" + dateRefit + "\" and insert_time = \"" + time + "\""
      jedis.set(time, sql)
      //      val incresql = "update shopflow_total_incre_pvuv " +
      //        " set pv = " + resultPvMap(time) +
      //        ", uv = " + resultUvMap(time) +
      //        ", rc = " + resultRcMap(time) +
      //        ", un = " + resultUnMap(time) +
      //        " where date = \"" + dateRefit + "\" and insert_time = \"" + time + "\""
    }
    jedis.close()

    //    result.foreachPartition { partition =>
    //      val jedis = RedisClient.pool.getResource
    //
    //
    //      val pvReduce = partition.map { line =>
    //        val date = line._1.toString()
    //        val time = line._2.substring(0, 5)
    //        val url = "lenovo.com"
    //        val cuc = line._4.toString()
    //        val rc = line._5.toInt
    //        val un = line._6.toString()
    //        (date + time + url, 1)
    //      }
    //
    //      jedis.select(14)
    //      partition.foreach { line =>
    //
    //        val date = line._1.toString()
    //        val time = line._2.substring(0, 5)
    //        val url = "lenovo.com"
    //        val cuc = line._4.toString()
    //        val rc = line._5.toInt
    //        val un = line._6.toString()
    //
    //
    //        jedis.hincrBy("PV:" + date + ":" + time + ":" + url, url, 1)
    //        jedis.sadd("UV:" + date + ":" + time + ":" + url, url + cuc)
    //
    //        if (rc.equals("0")) {
    //          jedis.hincrBy("RC:" + date + ":" + time + ":" + url, url, 1)
    //        }
    //        if (!un.equals("")) {
    //          jedis.hincrBy("UN:" + date + ":" + time + ":" + url, url, 1)
    //        }
    //      }
    //      jedis.close()
    //    }
  }


}


object LogPvUv1 extends Serializable {

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
}