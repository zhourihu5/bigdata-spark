package cn.itcast.spark.app.mock

import java.util.{Properties, UUID}

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

/**
 * 模拟产生用户使用百度搜索引擎时，搜索查询日志数据，包含字段为：
 *      uid, ip, search_datetime, search_keyword
 */
object MockSearchLogs {
    
    def main(args: Array[String]): Unit = {
    
        // 搜索关键词，直接到百度热搜榜获取即可
        val keywords: Array[String] = Array(
            "李心草溺亡案将择期宣判", "北京新增1例境外输入无症状病例",
            "王一博粉丝拿香菜应援", "美国纽约州发生枪击案", "安徽纸面服刑案当事人尚未被控制",
            "12种新冠治疗用药成准医保药品", "10月1日至4日加班发3倍工资", "美国白宫降半旗悼念金斯伯格",
            "乌鲁木齐地铁明天起恢复运营", "魏晨回应专业歌手不假唱", "大学生放《好日子》庆祝学校解封",
            "美国新冠肺炎超671万例", "保姆虐打85岁老人辩称是闹着玩", "拜仁8-0狂胜沙尔克"
        )
        
        // 发送Kafka Topic
        val props = new Properties()
        props.put("bootstrap.servers", "node1.itcast.cn:9092")
        props.put("acks", "1")
        props.put("retries", "3")
        props.put("key.serializer", classOf[StringSerializer].getName)
        props.put("value.serializer", classOf[StringSerializer].getName)
        val producer = new KafkaProducer[String, String](props)
        
        val random: Random = new Random()
        while (true){
            // 随机产生一条搜索查询日志
            val searchLog: SearchLog = SearchLog(
                getUserId(), //
                getRandomIp(), //
                getCurrentDateTime(), //
                keywords(random.nextInt(keywords.length)) //
            )
            println(searchLog.toString)
            Thread.sleep(100 + random.nextInt(100))
            
            val record = new ProducerRecord[String, String]("search-log-topic", searchLog.toString)
            producer.send(record)
        }
        // 关闭连接
        producer.close()
    }
    
    /**
     * 随机生成用户SessionId
     */
    def getUserId(): String = {
        val uuid: String = UUID.randomUUID().toString
        uuid.replaceAll("-", "").substring(16)
    }
    
    /**
     * 获取当前日期时间，格式为yyyyMMddHHmmssSSS
     */
    def getCurrentDateTime(): String = {
        val format =  FastDateFormat.getInstance("yyyyMMddHHmmssSSS")
        val nowDateTime: Long = System.currentTimeMillis()
        format.format(nowDateTime)
    }
    
    /**
     * 获取随机IP地址
     */
    def getRandomIp(): String = {
        // ip范围
        val range: Array[(Int, Int)] = Array(
            (607649792,608174079), //36.56.0.0-36.63.255.255
            (1038614528,1039007743), //61.232.0.0-61.237.255.255
            (1783627776,1784676351), //106.80.0.0-106.95.255.255
            (2035023872,2035154943), //121.76.0.0-121.77.255.255
            (2078801920,2079064063), //123.232.0.0-123.235.255.255
            (-1950089216,-1948778497),//139.196.0.0-139.215.255.255
            (-1425539072,-1425014785),//171.8.0.0-171.15.255.255
            (-1236271104,-1235419137),//182.80.0.0-182.92.255.255
            (-770113536,-768606209),//210.25.0.0-210.47.255.255
            (-569376768,-564133889) //222.16.0.0-222.95.255.255
        )
        // 随机数：IP地址范围下标
        val random = new Random()
        val index = random.nextInt(10)
        val ipNumber: Int = range(index)._1 + random.nextInt(range(index)._2 - range(index)._1)
        //println(s"ipNumber = ${ipNumber}")
        
        // 转换Int类型IP地址为IPv4格式
        number2IpString(ipNumber)
    }
    
    /**
     * 将Int类型IPv4地址转换为字符串类型
     */
    def number2IpString(ip: Int): String = {
        val buffer: Array[Int] = new Array[Int](4)
        buffer(0) = (ip >> 24) & 0xff
        buffer(1) = (ip >> 16) & 0xff
        buffer(2) = (ip >> 8) & 0xff
        buffer(3) = ip & 0xff
        // 返回IPv4地址
        buffer.mkString(".")
    }
    
}
