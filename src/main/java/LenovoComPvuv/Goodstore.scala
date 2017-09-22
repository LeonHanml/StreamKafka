package LenovoComPvuv


//import com.redis.RedisClient

import Model.CommonPvuv12
import Util.{KafkaConf, KafkaManager, MySqlPool, RedisClient, tools}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
//import org.apache.spark.implicts._
//import spark.implicits._
//activity 抽出方法

/** *
  * Goodstore
  *
  *
  * 单条存储数据
  */
object Goodstore {
  //@transent
  def main(args: Array[String]) {
    //.master("spark://10.250.100.17:7077")
    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.registerKryoClasses(Array(classOf[CommonPvuv12]))

    val spark = SparkSession.builder().appName("Goodstore.com").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").config(conf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

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

  /**
    * CLE good store filter 没有库存的商品名称 以及code
    *
    * @param spark
    * @param rdd
    */
  def processRdd(spark: SparkSession, rdd: RDD[(String, String)]): Unit = {

    val data = rdd.map(_._2)
    println("result-----")
    val result = data.filter(line => !line.toLowerCase().contains("spider") ).map(line => tools.dataSplitFromLine(line))
      .filter(dataMap => tools.isShopFlow(dataMap) && dataMap("WS").equals("10000001") && (dataMap.contains("CLE")) && (tools.exitShopStore(dataMap("CLE"))))
    println("result.collect()-----")
    println(result.collect())
    val part = result.map {
      dataMap =>

        val date = dataMap("date")
        val time = dataMap("time")

        var cleStr: String = if (dataMap.contains("CLE")) dataMap("CLE") else "CLE-NULL"

        val pattern = tools.regexOfgoodstore()
        val str = (pattern findAllIn cleStr).mkString
        val strArray = str.substring(2,str.length-2)split("::")

        println(strArray)
        val pcode = strArray(2)
        val state = strArray(3)
        val port = strArray(4)
//        Row(date,time,port, pcode, state.toInt)
        (port, pcode, state.toInt)
    }
    println(part.collect())
//    part.map(x=>(x,1)).reduceByKey(_+_).map(x=>x._1)
    val structType = StructType(Array(
//      StructField("date", StringType, true),
//      StructField("time", StringType, true),
      StructField("port", StringType, true),
      StructField("pcode", StringType, true),
      StructField("state", IntegerType, true)
    ))
    val partRow = part.map(x=>Row(x._1,x._2,x._3))
    println("开始注册temp table")
    val shopstoreDF = spark.createDataFrame(partRow, structType)
    shopstoreDF.createOrReplaceTempView("shopStoreFromLog")
    println("注册temp table结束 开始spark SQL")
    val sqlResult = spark.sql("SELECT a.goods_name, a.pcode, a.website_name, b.state , b.port " +
                              "FROM (SELECT tgba.goods_name,  tgba.goods_business_cd AS pcode, twc.website_name  " +
                                    "FROM shopdw.t03_goods_base_attr tgba " +
                                        "LEFT JOIN shopdw.t99_website_cd twc ON tgba.website_cd = twc.website_cd " +
                              ") a   " +
                              "right JOIN shopStoreFromLog b ON a.pcode = b.pcode")

    val sqlResultRdd = sqlResult.rdd
    println("sqlResultRdd--"+sqlResultRdd.collect())
    sqlResultRdd.foreachPartition { records => {
      if(!records.isEmpty){
        val conn = MySqlPool.getJdbcConn()
        val stmt = conn.createStatement()
        records.foreach{ record =>
//          val date = record.getAs("date")
//          val time = record.getAs("time")
          val goodsName = record.getAs("goods_name")
          val goodsCode = record.getAs("goods_code")
          val pcode = record.getAs("pcode")
          val websiteName = record.getAs("website_name")
          val state = record.getAs("state").toString.toInt
          val port = record.getAs("port")
//          val  update = s"update shopflow_goods_store_state set state = $state, date = '$date',min = '$date $time' where website_name = '$websiteName' and port = '$port' and pcode = '$pcode' and goods_name = '$goodsName'"
          val update = s"update shopflow_goods_store_state set state = $state where website_name = '$websiteName' and port = '$port' and pcode = '$pcode' and goods_name = '$goodsName'"
          val num = stmt.executeUpdate(update)
          if (num <= 0) {
            //            val insert = s"insert into shopflow_goods_store_state (date,website_name,port,pcode,goods_name,state,min) values" +
            //              s" ($date,'$websiteName','$port','$pcode','$goodsName',$state,'$date $time')"
            val insert = s"insert into shopflow_goods_store_state (website_name,port,pcode,goods_name,state) values" +
              s" ('$websiteName','$port','$pcode','$goodsName',$state)"
            stmt.executeUpdate(insert)
          }
        }
        MySqlPool.releaseConn(conn)
      }else{
        println("partition of rdd is null")
      }


    }



    }







//    result.foreachPartition { partition =>
//
//      val part = partition.map { dataMap =>
//
//        val date = dataMap("date")
//        val time = dataMap("time")
//
//        var cleStr: String = if (dataMap.contains("CLE")) dataMap("CLE") else "CLE-NULL"
//
//        val pattern = """.*\\|\\|show::goodsstore::[1-9]\d*::\d::wap\\|\\|.*""".r
//        val strArray = (pattern findAllIn cleStr).mkString.split("::")
//        val pcode = strArray(2)
//        val state = strArray(3)
//        val port = strArray(4)
//        Row(port, pcode, state.toInt)
//
//      }
//
//      val structType = StructType(Array(
//        StructField("port", StringType, true),
//        StructField("pcode", StringType, true),
//        StructField("state", IntegerType, true)
//      ))
//
//
//      //      spark.createDataFrame(part,structType)
//      val resultHiveDF = spark.sql("")
//

//    }
  }


}
