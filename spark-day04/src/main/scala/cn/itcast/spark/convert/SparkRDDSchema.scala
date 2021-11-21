package cn.itcast.spark.convert

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 自定义Schema方式转换RDD为DataFrame
 */
object SparkRDDSchema {
	
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
		
		// 2. 解析每行评分数据，封装至Row对象中
		// TODO: 第一步、RDD[Row]
		val rowsRDD: RDD[Row] = rawRatingsRDD.mapPartitions{ iter =>
			iter.map{line =>
				// 数据样本：654	370	2	887863914
				val Array(userId, itemId, rating, timestamp) = line.trim.split("\\t")
				// 返回实例对象
				Row(userId, itemId, rating.toDouble, timestamp.toLong)
			}
		}
		
		// 3. 自定义Schema信息，类型StructType
		// TODO: 第二步、Schema
		val schema: StructType = StructType(
			Array(
				StructField("user_id", StringType, nullable = true ),
				StructField("item_id", StringType, nullable = true ),
				StructField("rating", DoubleType, nullable = true ),
				StructField("timestamp", LongType, nullable = true )
			)
		)
		
		// 4. 结合RDD[Row]和Schema创建出DataFrame
		val ratingsDF: DataFrame = spark.createDataFrame(rowsRDD, schema)
		ratingsDF.printSchema()
		ratingsDF.show(20, truncate = false)
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
