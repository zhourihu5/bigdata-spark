package cn.itcast.spark.app.etl

import cn.itcast.spark.app.StreamingContextUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}

/**
 * 实时消费Kafka Topic数据，经过ETL（过滤、转换）后，保存至HDFS文件系统中，BatchInterval为：10s
 */
object StreamingETLHdfs {
	
	def main(args: Array[String]): Unit = {
		// 创建StreamingContext实例对象
		val ssc: StreamingContext = StreamingContextUtils.getStreamingContext(this.getClass, 10)
		
		// 从Kafka消费数据，采用New Consumer API方式
		val kafkaDStream: DStream[ConsumerRecord[String, String]] = StreamingContextUtils
			.consumerKafka(ssc, "search-log-topic")
		
		// 发布式缓存
		ssc.sparkContext.addFile("dataset/ip2region.db")
		
		// TODO: 对数据进行ETL：解析IP地址为省份和城市，最终保存至HDFS
		kafkaDStream.foreachRDD{(rdd, time) =>
			// 将batchTime进行转换：yyyy-MM-dd HH:mm:ss
			val batchTime = FastDateFormat.getInstance("yyyyMMddHHmmss")
				.format(time.milliseconds)
			
			// TODO: 当每批次RDD没有数据时，就终止程序运行
			if(!rdd.isEmpty()){
				// 数据样本：a5cff4db4bf42190,171.15.78.80,20200919172250596,美国白宫降半旗悼念金斯伯格
				val etlRDD: RDD[String] = rdd.mapPartitions{ iter =>
					// 创建DbSearch对象
					val dbSearcher: DbSearcher = new DbSearcher(new DbConfig(), SparkFiles.get("ip2region.db"))
					// 将每个分区数据中IP地址提取并解析为省份和城市
					iter
						// 提取topic中Message数据
						.map{record => record.value()}
						// 过滤
						.filter(msg => null != msg && msg.trim.split(",").length == 4)
						// 解析IP地址
						.map{msg =>
							val Array(_, ip, _, _) = msg.split(",")
							// 依据IP地址解析
							val dataBlock: DataBlock = dbSearcher.btreeSearch(ip)
							val region: String = dataBlock.getRegion
							val Array(_, _, province, city, _) = region.split("\\|")
							// 组合字符串
							s"$msg,$province,$city"
						}
				}
				
				// 保存ETL后RDD数据至HDFS文件系统
				etlRDD.coalesce(1).saveAsTextFile(s"datas/searchlogs/etl-$batchTime")
			}
		}
		
		// 启动流式应用
		ssc.start()
		ssc.awaitTermination()
		ssc.stop(stopSparkContext = true, stopGracefully = true)
	}
	
}
