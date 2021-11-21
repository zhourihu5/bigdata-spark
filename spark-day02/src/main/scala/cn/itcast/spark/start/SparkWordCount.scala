package cn.itcast.spark.start

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 基于Scala语言使用SparkCore编程实现词频统计：WordCount
 *      从HDFS上读取数据，统计WordCount，将结果保存到HDFS上
 */
object SparkWordCount {
	
	def main(args: Array[String]): Unit = {
		// TODO: 创建SparkContext实例对象，需要传递SparkConf对象，设置应用配置信息
		val sparkConf: SparkConf = new SparkConf()
    		.setAppName("SparkWordCount")
    		.setMaster("local[2]") // 设置运行本地模式
		val sc: SparkContext = new SparkContext(sparkConf)
		
		// TODO: 第一步、读取数据，封装数据至RDD集合
		val inputRDD: RDD[String] = sc.textFile("datas/wordcount.data")
		
		// TODO: 第二步、分析数据，调用RDD中函数
		val resultRDD: RDD[(String, Int)] = inputRDD
			// 将每行数据按照分隔符进行分割
			.flatMap(line => line.split("\\s+"))
			// 转换为二元组，表示每个单词出现一次
			.map(word => (word, 1))
			// 按照单词word分组，再进行组内聚合
			.reduceByKey((tmp, item) => tmp + item)
		
		// TODO: 第三步、保存数据，将最终RDD结果数据保存至外部存储系统
		resultRDD.foreach(tuple => println(tuple))
		
		// 应用运行结束， 关闭资源
		Thread.sleep(10000000)  // 线程休眠，查看4040监控页面
		sc.stop()
	}
	
	
}
