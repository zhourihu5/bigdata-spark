package cn.itcast.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 基于Scala语言使用SparkCore编程实现词频统计：WordCount
 *      从HDFS上读取数据，统计WordCount，将结果保存到HDFS上
 */
object SparkTopKey {
	
	def main(args: Array[String]): Unit = {
		// TODO: 创建SparkContext实例对象，需要传递SparkConf对象，设置应用配置信息
		val sparkConf: SparkConf = new SparkConf()
    		.setAppName("SparkTopKey")
    		.setMaster("local[2]") // 设置运行本地模式
		val sc: SparkContext = new SparkContext(sparkConf)
		
		// TODO: 第一步、读取数据，封装数据至RDD集合
		val inputRDD: RDD[String] = sc.textFile("/datas/wordcount.data")
		
		// TODO: 第二步、分析数据，调用RDD中函数
		val resultRDD: RDD[(String, Int)] = inputRDD
			// 将每行数据按照分隔符进行分割
			.flatMap(line => line.split("\\s+"))
			// 转换为二元组，表示每个单词出现一次
			.map(word => (word, 1))
			// 按照单词word分组，再进行组内聚合
			.reduceByKey((tmp, item) => tmp + item)
		
		// TODO: 第三步、保存数据，将最终RDD结果数据保存至外部存储系统
		/*
			(spark,4)
			(hive,3)
			(hadoop,2)
			(flink,1)
		 */
		//resultRDD.foreach(tuple => println(tuple))
		
		
		// 方式一：sortByKey，推荐使用
		resultRDD
			// 直接调用二元组中swap方法，将key和value互换
			.map(tuple => tuple.swap)
			.sortByKey(ascending = false) // 降序排序
			// 获取前3个数据
			.take(3)
			.foreach(tuple => println(tuple))
		
		println("========================================")
		
		// 方式二：sortBy
		/*
		  def sortBy[K](
		      f: (T) => K,
		      ascending: Boolean = true,
		      numPartitions: Int = this.partitions.length) : RDD[T]
		 */
		resultRDD
			// 指定词频count排序
			.sortBy(tuple => tuple._2, ascending = false)
			// 获取前3个数据
			.take(3)
			.foreach(tuple => println(tuple))
		
		println("========================================")
		
		
		// 方式三：top
		/*
		  def top(num: Int)(implicit ord: Ordering[T]): Array[T]
		 */
		resultRDD
			.top(3)(Ordering.by(tuple => tuple._2))
			.foreach(tuple => println(tuple))
		
		// 应用运行结束， 关闭资源
		sc.stop()
	}
	
	
}
