package cn.itcast.spark.sources

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * SparkSQL提供外部数据源加载数据：parquet、json、csv和rdbms
 */
object SparkSQLSourcesDemo {
	
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
		
		// TODO: 1. 加载parquet列式存储数据
		val usersDF: DataFrame = spark.read.parquet("datas/resources/users.parquet")
		//usersDF.show(10)
		// 使用默认的数据源：parquet，加载文件数据
		val df: DataFrame = spark.read.load("datas/resources/users.parquet")
		//df.show(10)
		
		// TODO: 2. 以文本格式加载Json数据
		val empDF = spark.read.json("datas/resources/employees.json")
		//empDF.show(10)
		val jsonDF: DataFrame = spark.read.text("datas/resources/employees.json")
		// TODO: 从json格式文件字符串提取数据
		// value =>  {"name":"Michael", "salary":3000}  -> name，  salary
		jsonDF
			.select(
				get_json_object($"value", "$.name").as("name"),
				get_json_object($"value", "$.salary").as("salary")
			)
			.show(10)
		
		// 3. 加载csv格式数据
		val ratingsDF: DataFrame = spark.read
			.option("header", "true")
			.option("sep", "\\t")
			.option("inferSchema", "true")
			.csv("datas/ml-100k/u.dat")
		//ratingsDF.show(10, truncate = false)
		
		// 4. 加载MySQL表的数据
		spark.read
			.format("jdbc")
			.option("driver", "com.mysql.cj.jdbc.Driver")
			.option("url", "jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
			.option("user", "root")
			.option("password", "123456")
			.option("dbtable", "db_test.emp")
			.load()
			.show(10, truncate = false)
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
