package cn.itcast.spark.func.cache

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object SparkCacheTest {
	
	def main(args: Array[String]): Unit = {
		
		// 构建Spark Application应用层入口实例对象
		val sc: SparkContext = {
			// a. 创建SparkConf对象，设置应用信息
			val sparkConf: SparkConf = new SparkConf()
				.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
				.setMaster("local[2]")
			// b. 传递SparkConf对象，创建实例
			SparkContext.getOrCreate(sparkConf)
		}
		
		// 1. 读取数据，封装为RDD
		val inputRDD: RDD[String] = sc.textFile("datas/wordcount.data")
		
		// TODO: 将RDD数据进行缓存
		inputRDD.persist(StorageLevel.MEMORY_AND_DISK)
		inputRDD.count() // 触发缓存执行
		
		inputRDD.foreach(println)
		
		// TODO： 当某个缓存RDD不在被使用时，是否资源
		//inputRDD.unpersist()
		
		
		// 应用结束，关闭资源
		Thread.sleep(1000000)
		sc.stop()
	}
}
