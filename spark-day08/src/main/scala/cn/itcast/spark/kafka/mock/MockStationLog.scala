package cn.itcast.spark.kafka.mock

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

/**
 * 模拟产生基站日志数据，实时发送Kafka Topic中，数据字段信息：
 *      基站标识符ID, 主叫号码, 被叫号码, 通话状态, 通话时间，通话时长
 */
object MockStationLog {
    
    def main(args: Array[String]): Unit = {
        // 发送Kafka Topic
        val props = new Properties()
        props.put("bootstrap.servers", "node1.itcast.cn:9092")
        props.put("acks", "1")
        props.put("retries", "3")
        props.put("key.serializer", classOf[StringSerializer].getName)
        props.put("value.serializer", classOf[StringSerializer].getName)
        val producer = new KafkaProducer[String, String](props)
        
        val random = new Random()
        val allStatus =Array(
            "fail", "busy", "barring", "success", "success", "success",
            "success", "success", "success", "success", "success", "success"
        )
        
        while (true){
            val callOut: String = "1860000%04d".format(random.nextInt(10000))
            val callIn: String = "1890000%04d".format(random.nextInt(10000))
            val callStatus: String = allStatus(random.nextInt(allStatus.length))
            val callDuration = if("success".equals(callStatus)) (1 + random.nextInt(10)) * 1000L else 0L
            
            // 随机产生一条基站日志数据
            val stationLog: StationLog = StationLog(
                "station_" + random.nextInt(10), //
                callOut, //
                callIn, //
                callStatus, //
                System.currentTimeMillis(), //
                callDuration //
            )
            println(stationLog.toString)
            Thread.sleep(10 + random.nextInt(100))
            
            val record = new ProducerRecord[String, String]("stationTopic", stationLog.toString)
            producer.send(record)
        }
        
        producer.close() // 关闭连接
    }
    
}
