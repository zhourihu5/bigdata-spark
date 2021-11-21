package cn.itcast.spark.test.load

import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadParquetTest {
	
	def main(args: Array[String]): Unit = {
		
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[4]")
			.config("spark.sql.shuffle.partitons", "4")
			.getOrCreate()
		import spark.implicits._
		
		// 加载parquet数据
		val dataframe: DataFrame = spark.read.parquet("dataset/pmt-etl")
		dataframe.printSchema()
		dataframe.show(10, truncate = false)
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
