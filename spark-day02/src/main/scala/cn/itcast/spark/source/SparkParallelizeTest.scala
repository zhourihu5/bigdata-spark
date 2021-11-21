package cn.itcast.spark.source

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark 采用并行化的方式构建Scala集合Seq中的数据为RDD
 */
object SparkParallelizeTest {
	
	def main(args: Array[String]): Unit = {
		
		// 构建SparkContext对象
		val sc: SparkContext = {
			// a. 创建SparkConf对象
			val sparkConf = new SparkConf()
    			.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    			.setMaster("local[2]")
			// b. 传递SparkConf对象，创建实例
			val context = SparkContext.getOrCreate(sparkConf) // 有就获取，没有创建
			// c. 返回实例对象
			context
		}
	
		// TODO: 创建本地集合
		val seq: Seq[Int] = Seq(1, 2, 3, 4, 5, 6, 7, 8)
		
		// 并行化本地集合，创建RDD
		/*
			def parallelize[T: ClassTag](
		      seq: Seq[T],
		      numSlices: Int = defaultParallelism
		    ): RDD[T]
		 */
		val inputRDD: RDD[Int] = sc.parallelize(seq, numSlices = 2)
		inputRDD.foreach(item => println(item))
		
		/*
		  def textFile(
		      path: String,
		      minPartitions: Int = defaultMinPartitions
		  ): RDD[String]
		 */
		sc.textFile("datas/wordcount.data",  minPartitions = 2)
		sc.textFile("datas/",  minPartitions = 2)
		
		// 应用结束，关闭资源
		sc.stop()
	}
	
}
