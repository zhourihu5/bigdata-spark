package cn.itcast.spark.convert

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 采用反射的方式将RDD转换为DataFrame和Dataset
 */
object SparkRDDInferring {
	
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
		
		val sc: SparkContext = spark.sparkContext
		
		// 1. 读取电影评分法数据，采用SparkContext
		val rawRatingsRDD: RDD[String] = sc.textFile("datas/ml-100k/u.data")
		
		// 2. 解析每行评分数据，封装至MovieRating对象中
		val ratingsRDD: RDD[MovieRating] = rawRatingsRDD.mapPartitions{ iter =>
			iter.map{line =>
				// 数据样本：654	370	2	887863914
				val Array(userId, itemId, rating, timestamp) = line.trim.split("\\t")
				// 返回实例对象
				MovieRating(userId, itemId, rating.toDouble, timestamp.toLong)
			}
		}
		
		// 3. 直接将RDD[CaseClass]转换为DataFrame
		val raingsDF: DataFrame = ratingsRDD.toDF()
		raingsDF.printSchema()
		raingsDF.show(10, truncate = false)
		
		// 4. 直接将RDD[CaseClass]转换为 Dataset
		// Dataset = RDD + Schema
		val ratingsDS: Dataset[MovieRating] = ratingsRDD.toDS()
		ratingsDS.printSchema()
		ratingsDS.show(10, truncate = false)
		
		spark.read.json("")
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
