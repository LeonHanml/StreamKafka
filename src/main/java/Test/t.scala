package Test

import Util.MySqlPool
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.datanucleus.store.rdbms.connectionpool.ConnectionPool

/**
  * Created by Administrator on 2017/9/19 0019.
  */
object OnlineTheTop3ItemForEachCategory2DB {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("OnlineForeachRDD2DB").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("/root/Documents/SparkApps?checkpoint")

    val userClickLogsDStream = ssc.socketTextStream("Master", 9999)

    val formattedUserClickLogDStream = userClickLogsDStream.map(clickLog =>
      (clickLog.split(" ")(2) +"_" + clickLog.split(" ")(1), 1))

    val categoryUserClickLogsDStream = formattedUserClickLogDStream.reduceByKeyAndWindow(_ + _ ,
      _ - _ ,Seconds(60),Seconds(20))

    categoryUserClickLogsDStream.foreachRDD{rdd =>{

      if(rdd.isEmpty()){
        print("No data inputted!!!")
      }else {
        val categoryItemRow = rdd.map(reducedItem => {
          val category = reducedItem._1.split("_")(0)
          val item = reducedItem._1.split("——")(1)
          val click_count = reducedItem._2
          Row(category, item, click_count)
        })
        val structType = StructType(Array(
          StructField("category", StringType, true),
          StructField("item", StringType, true),
          StructField("click_count", IntegerType, true)
        ))

        val hiveContext = new HiveContext(rdd.context)

        val categoryItemDF = hiveContext.createDataFrame(categoryItemRow, structType)

        categoryItemDF.registerTempTable("categoryItemTable")

        val resultDataFrame = hiveContext.sql("SELECT category,item,click_count FROM (SELECT category,item,click_count,row_number()" +
          "OVER(PARTITION BY category ORDER BY click_count DESC) rank FROM categoryItemTable) subquery " +
          "WHERE rank <= 3")

        resultDataFrame.show()

        val resultRowRDD = resultDataFrame.rdd


        resultRowRDD.foreachPartition { partitionOfRecords => {

          if(partitionOfRecords.isEmpty){
            println("this is RDD is not null but partition is null")
          }else{
            val connection = MySqlPool.getJdbcConn()
            partitionOfRecords.foreach(record => {
              val sql = "insert into categorytop3(category,item,client_count) values('" + record.getAs("category") + "','" +
                record.getAs("item") + "','" + record.getAs("click_count") + ")"
              val stmt = connection.createStatement
              stmt.executeUpdate(sql)
            })
            MySqlPool.releaseConn(connection)
          }

        }
        }
      }
    }
    }

    ssc.start()
    ssc.awaitTermination()
  }


}