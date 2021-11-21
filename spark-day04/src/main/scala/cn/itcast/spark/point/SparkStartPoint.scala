package cn.itcast.spark.point

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * 使用SparkSession加载文本数据，TODO：如何创建SparkSession实例对象
 */
object SparkStartPoint {
	
	def main(args: Array[String]): Unit = {
		// TODO： 构建SparkSession实例对象，采用建造者设计模式
		val spark: SparkSession = SparkSession
			.builder()
			// 设置应用名称
    		.appName(this.getClass.getSimpleName.stripSuffix("$"))
			// 设置运行Master
    		.master("local[2]")
    		.getOrCreate()
		// 导入隐式转换函数库
		import spark.implicits._
		
		// TODO: 使用SparkSession加载文本数据，封装至DataFrame中
		// DataFrame = RDD[Row] + Schema（名称和类型）
		// Dataset = RDD + Schema 、Dataset[Row] = DataFrame
		val inputDS: Dataset[String] = spark.read.textFile("datas/wordcount.data")
		// 获取Schema信息：字段名称和字段类型
		/*
		root
		 |-- value: string (nullable = true)
		 */
		inputDS.printSchema()
		// 获取前N条样本数据
		/*
			+--------------------+
			|               value|
			+--------------------+
			|   hadoop spark h...|
			|mapreduce spark  ...|
			|hive spark hadoop...|
			|spark hive sql sq...|
			|                    |
			|hdfs hdfs mapredu...|
			|                    |
			|                    |
			+--------------------+
		 */
		inputDS.show(10)
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
