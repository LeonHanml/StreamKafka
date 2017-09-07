package Test
import scala.io.Source
import scala.collection.immutable.Map
import scala.collection.JavaConversions._
import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by Administrator on 2017/8/18 0018.
  */

object Test extends  Serializable {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("AppName")

    val sc = new SparkContext(sparkConf)
    Test.run("20170807")
  }
  def run(date: String): Unit= {
    //    var str = Tool.dateToStamp("2012-02-22 12:23:34")
    // val rdd = sc.textFile("hdfs://10.250.100.47:8020/flume/2017/08/01/11/traffic*")
    // date=20170811
    val year = date.substring(0, 4)
    val month = date.substring(4, 6)
    val day = date.substring(6, 8)
    val filePath = "hdfs://10.250.100.47:8020/flume/" + year + "/" + month + "/" + day + "/*/traffic*"
    // val file = "hdfs://10.250.100.47:8020/flume/2017/07/25/*/traffic*"

//    val rdd = sc.textFile(filePath)
    // for(i <- Range(0,2)){
    //      println(rdd.)
    // }
    // println(rdd.count())//4136672
//    val result = rdd.filter(line => (!line.toLowerCase().contains("spider"))).map(line => tools1.dataSplitFromLine(line)).filter(dataMap => tools1.isShopFlow(dataMap) && dataMap("WS").equals("10000001") && (!dataMap.contains("CLE")))
//    println(result.count())
  }
}
object tools1 extends Serializable{

  def dataSplitFromLine(line: String): scala.collection.mutable.Map[String, String] = {
    val dataMap: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
    var vars = line.split("\\|_\\|")
    val datetime = vars(2).split("T")
    val date = datetime(0).mkString
    val time = datetime(1).split("\\+")(0).mkString.substring(0, 5) + ":00"
    dataMap("date") = date
    dataMap("time") = time
    val urlString = vars(3)
    //    val dataMap: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
    for (data <- urlString.split("\\?")(1).split("&")) {
      val dataList = data.split("=")
      if (dataList.size == 2) {
        dataMap(dataList(0)) = dataList(1)
      } else if (dataList.size == 1) {
        dataMap(dataList(0)) = "NULL"
      }
    }
    return dataMap
  }

  def dateToStamp(dateString: String): String = {
    var res = ""
    var simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var date = simpleDateFormat.parse(dateString)
    var ts = date.getTime()
    res = String.valueOf(ts);
    return res
  }

  def isShopFlow(dataMap: scala.collection.mutable.Map[String, String] ): Boolean ={

    var shopFlow = false
    try{
      val pagesite =if (dataMap.contains("PS")) dataMap("PS") else "PS-NULL"
      val pu = if (dataMap.contains("PU")) dataMap("PU") else "PU-NULL"
      val pageurl = java.net.URLDecoder.decode(java.net.URLDecoder.decode(java.net.URLDecoder.decode(pu)))

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

    }catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    return  shopFlow
  }
}


