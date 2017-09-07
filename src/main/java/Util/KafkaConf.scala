package Util

import scala.collection.immutable.Map

/**
  * Created by Administrator on 2017/7/26 0026.
  */
object KafkaConf {

  val topicsSet = Set("traffic")
  val brokers = "10.250.100.47:9093,10.250.100.53:9093,10.250.100.55:9093"
  val groupId =  "activity9_pvuv"
  val kafkaParams = Map[String, String](
    "metadata.broker.list" -> brokers,
    "group.id" -> groupId,
    "serializer.class" -> "kafka.serializer.StringEncoder")

}
