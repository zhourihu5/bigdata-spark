package cn.itcast.spark.source

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 数据源：Rate Source，以每秒指定的行数生成数据，每个输出行包含一个timestamp和value。
 */
object StructuredRateSource {
	
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
		
		// TODO：从Rate数据源实时消费数据
		val rateStreamDF: DataFrame = spark.readStream
			.format("rate")
			.option("rowsPerSecond", "10") // 每秒生成数据数目
			.option("rampUpTime", "0s") // 每条数据生成间隔时间
			.option("numPartitions", "2") // 分区数目
			.load()
		/*
		
		 */
		//rateStreamDF.printSchema()
		
		
		// 3. 设置Streaming应用输出及启动
		val query: StreamingQuery = rateStreamDF.writeStream
			// 设置输出模式：Append表示新数据以追加方式输出
			.outputMode(OutputMode.Append())
			.format("console")
			.option("numRows", "100")
			.option("truncate", "false")
			// 流式应用，需要启动start
			.start()
		// 流式查询等待流式应用终止
		query.awaitTermination()
		// 等待所有任务运行完成才停止运行
		query.stop()
	}
	
}
