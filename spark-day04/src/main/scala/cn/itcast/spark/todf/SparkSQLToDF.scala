package cn.itcast.spark.todf

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 隐式调用toDF函数，将数据类型为元组的Seq和RDD集合转换为DataFrame
 */
object SparkSQLToDF {
	
	def main(args: Array[String]): Unit = {
		
		// 构建SparkSession实例对象，采用建造者设计模式
		val spark: SparkSession = SparkSession
			.builder()
			// 设置应用名称
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			// 设置运行Master
			.master("local[2]")
			.getOrCreate()
		// 导入隐式转换函数库
		import spark.implicits._
		
		// TODO: RDD[元组]
		val df: DataFrame = spark.sparkContext.parallelize(
			Seq(
				(10001, "zhangsan", 23),
				(10002, "lisi", 22),
				(10003, "wangwu", 23),
				(10004, "zhaoliu", 24)
			),
			numSlices = 2
		).toDF("id", "name", "age")
		df.printSchema()
		df.show(10, truncate = false)
		
		println("===================================================")
		// TODO: Seq[元组]
		val dataframe = Seq(
			(10001, "zhangsan", 23),
			(10002, "lisi", 22),
			(10003, "wangwu", 23),
			(10004, "zhaoliu", 24)
		).toDF("id", "name", "age")
		dataframe.printSchema()
		dataframe.show(10, truncate = false)
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
