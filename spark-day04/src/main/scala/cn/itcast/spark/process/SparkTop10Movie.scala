package cn.itcast.spark.process

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * 需求：对电影评分数据进行统计分析，获取Top10电影（电影评分平均值最高，并且每个电影被评分的次数大于2000)
 */
object SparkTop10Movie {
	
	def main(args: Array[String]): Unit = {
		
		// T构建SparkSession实例对象，采用建造者设计模式
		val spark: SparkSession = SparkSession
			.builder()
			// 设置应用名称
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			// 设置运行Master
			.master("local[4]")
			// TODO: 设置shuffle时分区数目
			.config("spark.sql.shuffle.partitions", "4")
			.getOrCreate()
		// 导入隐式转换函数库
		import spark.implicits._
		
		// TODO: 1. 读取电影评分数据，从本地文件系统读取
		val rawRatingRDD: RDD[String] = spark.sparkContext.textFile("datas/ml-1m/ratings.dat", minPartitions = 2)
		//println(s"Count = ${rawRatingRDD.count()}")
		//println(rawRatingRDD.first())
		
		// TODO: 2. 转换数据，指定Schema信息，封装到DataFrame
		val ratingDF: DataFrame = rawRatingRDD
			.map{line =>
				// 样本数据：1::1193::5::978300760
				val Array(userId, movieId, rating, timestamp) = line.split("::")
				// 返回四元组
				(userId, movieId, rating.toDouble, timestamp.toLong)
			}
			.toDF("userId", "movieId", "rating", "timestamp")
		/*
			root
			 |-- userId: string (nullable = true)
			 |-- movieId: string (nullable = true)
			 |-- rating: double (nullable = false)
			 |-- timestamp: long (nullable = false)
		 */
		//ratingDF.printSchema()
		//ratingDF.show(10, truncate = false)
		
		// 当某个DataFrame被使用多次，考虑缓存，底层就是RDD
		ratingDF.persist(StorageLevel.MEMORY_AND_DISK).count()
		
		// TODO: 3. 基于SQL方式分析
		// step1: 注册为临时视图
		ratingDF.createOrReplaceTempView("tmp_view_ratings")
		// step2: 编写SQL并执行
		/*
			- 1、按照电影进行分组，计算每个电影平均评分和评分人数
			- 2、过滤获取评分人数大于2000的电影
			- 3、按照电影评分降序排序，再按照评分人数排序
		 */
		val top10MoviesDF: DataFrame = spark.sql(
			"""
			  |SELECT
			  |  movieId,
			  |  ROUND(AVG(rating), 2) AS avg_rating,
			  |  COUNT(movieId) AS cnt_rating
			  |FROM
			  |  tmp_view_ratings
			  |GROUP BY
			  |  movieId
			  |HAVING
			  |  cnt_rating >= 2000
			  |ORDER BY
			  |  avg_rating DESC, cnt_rating DESC
			  |LIMIT
			  |  10
			  |""".stripMargin)
		//top10MoviesDF.printSchema()
		//top10MoviesDF.show(10, truncate = false)
		
		// TODO: 4. 基于DSL方式分析
		// 导入函数库
		import org.apache.spark.sql.functions._
		val resultDF: Dataset[Row] = ratingDF
			// a. 按照电影分组
			.groupBy($"movieId")
			// b. 聚合操作：平均评分、评分人数
			.agg(
				// ROUND(AVG(rating), 2) AS avg_rating
				round(avg($"rating"), 2).as("avg_rating"), //
				count($"movieId").as("cnt_rating")
			)
			// c. 过滤 评分人数大于2000
			.where($"cnt_rating" >= 2000)
			// d. 排序
			.orderBy($"avg_rating".desc, $"cnt_rating".desc)
			// e. 获取前10
			.limit(10)
		//resultDF.printSchema()
		//resultDF.show(10, truncate = false)
		
		// 数据不再使用，释放缓存
		ratingDF.unpersist()
		
		// TODO: 5. 保存结果数据至MySQL表和CSV文件
		resultDF.cache()  // 由于结果数据很少，直接放入内存即可
		
		// TODO: 5.1 保存数据至MySQL表中
		resultDF
			.write
			.mode(SaveMode.Overwrite) // 保存模式
			.option("driver", "com.mysql.cj.jdbc.Driver")
			.option("user", "root")
			.option("password", "123456")
			.jdbc(
				"jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
				"db_test.tb_top10_movies",
				new Properties()
			)
		
		// TODO: 5.2 保存数据至CSV文件 -> 每行数据中各个字段使用逗号隔开, 往往CSV文件首行为列名称
		resultDF
            .write
            .mode(SaveMode.Overwrite)
    		.option("header", "true")
			.csv("datas/top10Movies/")
		
		// 释放资源，清除数据
		resultDF.unpersist()
		
		// 应用结束，关闭资源
		Thread.sleep(1000000)
		spark.stop()
	}
	
}
