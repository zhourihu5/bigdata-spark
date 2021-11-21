package cn.itcast.spark.start

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 使用SparkSQL中DSL编程方式实现词频统计WordCount
 */
object SparkDSLWordCount {
	
	def main(args: Array[String]): Unit = {
		// 构建SparkSession实例对象，采用建造者设计模式
		val spark: SparkSession = SparkSession
			.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
    		.master("local[2]")
			// 设置运行Master
			.getOrCreate()
		// 导入隐式转换函数库
		import spark.implicits._
		
		// 使用SparkSession加载文本数据，封装至DataFrame中
		val inputDS: Dataset[String] = spark.read.textFile("datas/wordcount.data")
		/*
		root
		 |-- value: string (nullable = true)
		 */
		//inputDS.printSchema()
		//inputDS.show(10)
		
		// TODO: 采用DSL分析数据 -> 调用Dataset API分析数据
		val dataset: Dataset[String] = inputDS
			// a. 过滤数据：null, 空字符串
			.filter(line => null != line && line.trim.length > 0)
			// b. 对每行数据分割单词
			.flatMap(line => line.trim.split("\\s+"))
		/*
		root
		 |-- value: string (nullable = true)
		 */
		//dataset.printSchema()
		//dataset.show()
		
		/*
			表：tb_words   字段名称: word
			SQL:
				SELECT word, COUNT(1) AS total FROM tb_words GROUP BY word
		 */
		val resultDF: DataFrame = dataset.groupBy($"value").count()
		/*
			root
			 |-- value: string (nullable = true)
			 |-- count: long (nullable = false)
		 */
		resultDF.printSchema()
		/*
			+---------+-----+
			|value    |count|
			+---------+-----+
			|sql      |2    |
			|spark    |11   |
			|mapreduce|4    |
			|hdfs     |2    |
			|hadoop   |3    |
			|hive     |6    |
			+---------+-----+
		 */
		resultDF.show(10, truncate = false)
		
		// 应用结束，关闭资源
		Thread.sleep(1000000)
		spark.stop()
	}
	
}
