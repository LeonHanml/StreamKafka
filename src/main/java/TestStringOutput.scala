import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import refitPvuv.processRdd

/**
  * Created by Administrator on 2017/8/2 0002.
  */
object TestStringOutput {
  def main(args: Array[String]): Unit = {
    refit("20170725")
  }

  def refit(date: String): Unit = {
//    val spark = SparkSession.builder().appName("1000001_Lenovo.comPvuv_Refit").enableHiveSupport().getOrCreate()
//    val sc = spark.sparkContext
    //  val rdd = sc.textFile("hdfs://10.250.100.47:8020/flume/2017/08/01/11/traffic*")

    val year = date.substring(0, 4)
    val month = date.substring(4, 6)
    val day = date.substring(6, 8)
    val filePath = "hdfs://10.250.100.47:8020/flume/" + year + "/" + month + "/" + day + "/*/traffic*"
    println(filePath)
//    val rdd = sc.textFile(filePath)

//    processRdd(rdd, date)

  }
  def t (args: Array[String]) {

    val conf = new SparkConf().setMaster("local[*]").setAppName("MapTest")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 注册要序列化的自定义类型。
    conf.registerKryoClasses(Array(classOf[StringPojo]))
    val context = new SparkContext(conf)
    val lineRdd = context.textFile("E:\\sourceData\\maptest.txt")
    //    lineRdd.distinct()
    val wordsRdd=lineRdd.map{line =>
      val words=line.split("\t")
      new StringPojo(words(0),words(1))
    }

    val pairRdd1= wordsRdd.map(pojo=>(pojo.name+pojo.secondName,1))
    pairRdd1.reduceByKey(_+_).foreach(println)
    val pairRdd= wordsRdd.map(pojo=>(pojo.name+pojo.secondName,pojo))
    pairRdd.reduceByKey((x,y)=>x).map(_._2).foreach(println)

    //    pairRdd.distinct().foreach(println)
    //    val distRdd=wordsRdd.distinct()
    //    distRdd.foreach(println)
  }


}
class StringPojo(val name:String,val secondName:String) {
  override def toString: String = {
    super.toString
    this.name + "|" + this.secondName
  }
  override def hashCode(): Int = {
    //    println("name:"+ this.secondName+",hashCode:"+ this.secondName.hashCode)
    this.secondName.hashCode
  }
}