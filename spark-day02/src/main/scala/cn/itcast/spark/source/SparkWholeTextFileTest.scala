package cn.itcast.spark.source

import org.apache.spark.rdd.RDD

/**
 * 采用SparkContext#wholeTextFiles()方法读取小文件
 */
object SparkWholeTextFileTest {
	
	def main(args: Array[String]): Unit = {
		
		// 构建SparkContext对象
		import org.apache.spark.{SparkConf, SparkContext}
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
		
		// TODO: 读取小文件数据
		/*
		  def wholeTextFiles(
		      path: String,
		      minPartitions: Int = defaultMinPartitions
		  ): RDD[(String, String)]
		 */
		val inputRDD: RDD[(String, String)] = sc.wholeTextFiles("datas/ratings100", minPartitions = 2)
		println(s"RDD 分区数目 = ${inputRDD.getNumPartitions}")
		
		// 打印样本数据
		inputRDD.take(1).foreach(item => println(item))
		
		
		// 应用结束，关闭资源
		sc.stop()
	}
	
}
