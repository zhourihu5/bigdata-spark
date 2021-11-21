package cn.itcast.spark.sink

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 使用Structured Streaming从TCP Socket实时读取数据，进行词频统计，将结果保存至MySQL表中
 */
object StructuredMySQLSink {
	
	def main(args: Array[String]): Unit = {
		
		// 1. 构建SparkSession实例对象，加载流式数据
		val spark: SparkSession = SparkSession.builder()
    		.appName(this.getClass.getSimpleName.stripSuffix("$"))
    		.master("local[2]")
    		.config("spark.sql.shuffle.partitions", "2")
    		.getOrCreate()
		import spark.implicits._
		
		// 2. 从TCP Socket读取流式数据
		val inputStreamDF: DataFrame = spark.readStream
			.format("socket")
			.option("host", "node1.itcast.cn")
			.option("port", 9999)
			.load()
		
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
			// 设置插入名称
    		.queryName("query-wordcount")
			// 设置触发器
    		.trigger(Trigger.ProcessingTime("5 seconds"))
			// TODO: 使用foreach，将数据保存至MySQL表中
			// def foreach(writer: ForeachWriter[T]): DataStreamWriter[T]
    		.foreach(new MySQLForeachWriter())
			// 设置检查点目录
			.option("checkpointLocation", "datas/structured/ckpt-wordcount001")
			.start() // 启动流式查询
		query.awaitTermination()
		query.stop()
	}
	
}
