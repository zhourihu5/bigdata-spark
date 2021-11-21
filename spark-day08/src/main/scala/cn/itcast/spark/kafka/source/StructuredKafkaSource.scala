package cn.itcast.spark.kafka.source

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 使用Structured Streaming从Kafka实时读取数据，进行词频统计，将结果打印到控制台。
 */
object StructuredKafkaSource {
	
	def main(args: Array[String]): Unit = {
		
		// 1. 构建SparkSession实例对象，加载流式数据
		val spark: SparkSession = SparkSession.builder()
    		.appName(this.getClass.getSimpleName.stripSuffix("$"))
    		.master("local[2]")
    		.config("spark.sql.shuffle.partitions", "2")
    		.getOrCreate()
		import spark.implicits._
		
		// 2. 从Kafka 读取流式数据
		val kafkaStreamDF: DataFrame = spark.readStream
			.format("kafka")
			.option("kafka.bootstrap.servers", "node1.itcast.cn:9092")
			.option("subscribe", "wordsTopic")
			.load()
		val inputStreamDF: Dataset[String] = kafkaStreamDF
			.selectExpr("CAST(value AS STRING)")
			.as[String]
		
		// 3. 调用DataFrame API处理分析数据
		val resultStreamDF: DataFrame = inputStreamDF
			.as[String] // 将DataFrame转换为Dataset
			.filter(line => null != line && line.trim.length > 0)
			.flatMap(line => line.trim.split("\\s+"))
			// 按照单词分组，进行统计
			.groupBy($"value").count()
		
		// 4. 将结果数据进行输出
		val query: StreamingQuery = resultStreamDF.writeStream
			// Update 更新模式，当ResultTable中更新的数据进行输出，类似mapWithState
			.outputMode(OutputMode.Update())
			// TODO: 设置插入名称
    		.queryName("query-wordcount")
			.format("console")
			.option("numRows", "20")
			.option("truncate", "false")
			// TODO: 设置检查点目录
			.option("checkpointLocation", s"datas/structured/ckpt-kafka-${System.nanoTime()}")
			.start() // 启动流式查询
		query.awaitTermination()
		query.stop()
	}
	
}
