package Util

import java.text.SimpleDateFormat

import scala.collection.mutable.Map
import scala.util.matching.Regex
object tools extends Serializable {
  def dataSplit(urlString: String): Map[String, String] = {
    val dataMap: Map[String, String] = Map()
    for (data <- urlString.split("\\?")(1).split("&")) {
      val dataList = data.split("=")
      if (dataList.size == 2) {
        dataMap(dataList(0)) = dataList(1)
      } else if (dataList.size == 1) {
        dataMap(dataList(0)) = ""
      }
    }
    return dataMap
  }

  def dataSplitFromLine(line: String): Map[String, String] = {
    val dataMap: Map[String, String] = Map()
    var vars = line.split("\\|_\\|")
    val datetime = vars(2).split("T")
    val date = datetime(0).mkString
    val time = datetime(1).split("\\+")(0).mkString.substring(0, 5) + ":00"
    dataMap("date") = date
    dataMap("time") = time
    val urlString = vars(3)
    //    val dataMap: Map[String, String] = Map()
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


  def isShopFlow(dataMap:Map[String, String]): Boolean = {
    //    ,pagesite:String,pageurl:String.
    var shopFlow = false
    try {
      val pagesite = if (dataMap.contains("PS")) dataMap("PS") else "PS-NULL"
      val pu = if (dataMap.contains("PU")) dataMap("PU") else "PU-NULL"
      val pageurl = urlDecoder(pu)

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
    return shopFlow

  }

  def urlDecoder(str: String): String = {


    try {
      return java.net.URLDecoder.decode(java.net.URLDecoder.decode(java.net.URLDecoder.decode(str)))
    } catch {
      case ex: IllegalArgumentException => {
        return "NULL"
      }
    }

  }

  def isActivity(dataMap: Map[String, String]): Boolean = {
    //    ,pagesite:String,pageurl:String.
    var activity = false
    try {

      val pu = if (dataMap.contains("PU")) dataMap("PU") else "PU-NULL"
      val tt = if (dataMap.contains("TT")) dataMap("TT") else "TT-NULL"
      val ttDecoder = urlDecoder(tt)
      val pageurl = urlDecoder(pu)

      activity = pageurl.contains("/activity/")


    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    activity

  }

  def exitShopStore(str:String): Boolean ={
    val pattern  = regexOfgoodstore()

    val strs = (pattern findAllIn str).mkString
    if (!strs .equals("")){
      return true
    }else{
      return false
    }
  }
  def regexOfgoodstore(): Regex ={
    val pattern = """\|\|show::goodsstore::[1-9]\d*::\d::[a-z]+\|\|""".r
    return pattern
  }


}