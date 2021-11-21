package cn.itcast.spark.start

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}

/**
 * 使用Structured Streaming从TCP Socket实时读取数据，进行词频统计，将结果打印到控制台。
 */
object StructuredWordCount {
	
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
		/*
		root
		 |-- value: string (nullable = true)
		 
		 +-----------------+
		|value            |
		+-----------------+
		|spark spark spark|
		+-----------------+
		 */
		//inputStreamDF.printSchema()
		
		// 3. 调用DataFrame API处理分析数据
		val resultStreamDF: DataFrame = inputStreamDF
			.as[String] // 将DataFrame转换为Dataset
			.filter(line => null != line && line.trim.length > 0)
			.flatMap(line => line.trim.split("\\s+"))
			// 按照单词分组，进行统计
			.groupBy($"value").count()
		
		// 4. 将结果数据进行输出
		val query: StreamingQuery = resultStreamDF.writeStream
			// Append 追加模式，将数据直接输出，没有任何聚合操作
    		//.outputMode(OutputMode.Append())
			// Complete 完全模式，将ResultTable中所有结果数据进行输出，类似updateStateByKey
    		//.outputMode(OutputMode.Complete())
			// Update 更新模式，当ResultTable中更新的数据进行输出，类似mapWithState
			.outputMode(OutputMode.Update())
			.format("console")
    		.option("numRows", "20")
    		.option("truncate", "false")
			.start() // 启动流式查询
		query.awaitTermination()
		query.stop()
	}
	
}
