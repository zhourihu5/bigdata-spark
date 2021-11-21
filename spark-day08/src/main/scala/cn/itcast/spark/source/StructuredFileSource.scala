package cn.itcast.spark.source

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/**
 * 使用Structured Streaming从目录中读取文件数据：统计年龄小于25岁的人群的爱好排行榜
 */
object StructuredFileSource {
	
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
		
		// TODO: 从文件系统，监控目录，读取CSV格式数据
		// 数据样本 -> jack;23;running
		val csvSchema: StructType = new StructType()
			.add("name", StringType, nullable = true)
			.add("age", IntegerType, nullable = true)
			.add("hobby", StringType, nullable = true)
		val inputStreamDF: DataFrame = spark.readStream
			.option("sep", ";")
			.option("header", "false")
			// 指定schema信息
			.schema(csvSchema)
			.csv("file:///D:/datas/")
		
		// 依据业务需求，分析数据：统计年龄小于25岁的人群的爱好排行榜
		val resultStreamDF: Dataset[Row] = inputStreamDF
			// 年龄小于25岁
			.filter($"age" < 25)
			// 按照爱好分组统计
			.groupBy($"hobby").count()
			// 按照词频降序排序
			.orderBy($"count".desc)
		// 设置Streaming应用输出及启动
		val query: StreamingQuery = resultStreamDF.writeStream
			// 对流式应用输出来说，设置输出模式
			.outputMode(OutputMode.Complete())
			.format("console")
			.option("numRows", "10")
			.option("truncate", "false")
			// 流式应用，需要启动start
			.start()
		// 查询器等待流式应用终止
		query.awaitTermination()
		query.stop() // 等待所有任务运行完成才停止运行
	}
	
}
