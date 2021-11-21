package cn.itcast.spark.deduplication

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

object StructuredDeduplication {
	
	def main(args: Array[String]): Unit = {
		
		// 构建SparkSession实例对象
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[2]")
			// 设置Shuffle分区数目
			.config("spark.sql.shuffle.partitions", "2")
			.getOrCreate()
		// 导入隐式转换和函数库
		import spark.implicits._
		import org.apache.spark.sql.functions._
		
		// 1. 从TCP Socket 读取数据
		val inputTable: DataFrame = spark.readStream
			.format("socket")
			.option("host", "node1.itcast.cn")
			.option("port", 9999)
			.load()
		
		// 2. 数据处理分析
		val resultTable: DataFrame = inputTable
			.as[String]
			.filter(line => null != line && line.trim.length > 0)
			// 样本数据：{“eventTime”: “2016-01-10 10:01:50”,“eventType”: “browse”,“userID”:“1”}
			.select(
				get_json_object($"value", "$.eventTime").as("event_time"), //
				get_json_object($"value", "$.eventType").as("event_type"), //
				get_json_object($"value", "$.userID").as("user_id")//
			)
			// TODO：按照UserId和EventType去重
			.dropDuplicates("user_id","event_type")
			.groupBy($"user_id", $"event_type")
			.count()
		
		// 3. 设置Streaming应用输出及启动
		val query: StreamingQuery = resultTable.writeStream
			.outputMode(OutputMode.Complete())
			.format("console")
			.option("numRows", "10")
			.option("truncate", "false")
			.start()
		query.awaitTermination() // 流式查询等待流式应用终止
		query.stop()
	}
	
}
