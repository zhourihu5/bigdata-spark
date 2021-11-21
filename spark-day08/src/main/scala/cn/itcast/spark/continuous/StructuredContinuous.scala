package cn.itcast.spark.continuous

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 从Spark 2.3版本开始，StructuredStreaming结构化流中添加新流式数据处理方式：Continuous processing
 *  持续流数据处理：当数据一产生就立即处理，类似Storm、Flink框架，延迟性达到100ms以下，目前属于实验开发阶段
 */
object StructuredContinuous {
	
	def main(args: Array[String]): Unit = {
		
		// 1. 构建SparkSession实例对象，加载流式数据
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[3]")
			.config("spark.sql.shuffle.partitions", "3")
			.getOrCreate()
		import spark.implicits._
		
		// 2. 从Kafka 读取流式数据
		val kafkaStreamDF: DataFrame = spark.readStream
			.format("kafka")
			.option("kafka.bootstrap.servers", "node1.itcast.cn:9092")
			.option("subscribe", "stationTopic")
			.load()
		
		// 3. 对topic数据进行ETL操作
		val etlStreamDF = kafkaStreamDF
			.selectExpr("CAST(value AS STRING)")
			.as[String]
			// 过滤数据: station_7,18600008767,18900002601,success,1600683711964,7000
    		.filter{msg =>
			    null != msg && msg.trim.split(",").length == 6 && "success".equals( msg.trim.split(",")(3))
		    }
		
		// 3. 针对流式应用来说，输出的是流
		val query: StreamingQuery = etlStreamDF.writeStream
			// 对流式应用输出来说，设置输出模式
			.outputMode(OutputMode.Append())
			.format("kafka")
			.option("kafka.bootstrap.servers", "node1.itcast.cn:9092")
			.option("topic", "etlTopic")
			// TODO: 设置流计算模式为连续处理
    		.trigger(Trigger.Continuous("1 second"))
			// 设置检查点目录
			.option("checkpointLocation", s"datas/structured/kafka-etl-10099")
			// 流式应用，需要启动start
			.start()
		query.awaitTermination()
		query.stop()
	}
	
}
