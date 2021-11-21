package cn.itcast.spark.func.agg

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD中聚合函数，针对RDD中数据类型Key/Value对：
 * groupByKey
 * reduceByKey/foldByKey
 * aggregateByKey
 * combineByKey
 */
object SparkAggByKeyTest {
	
	def main(args: Array[String]): Unit = {
		// 创建应用程序入口SparkContext实例对象
		val sc: SparkContext = {
			// 1.a 创建SparkConf对象，设置应用的配置信息
			val sparkConf: SparkConf = new SparkConf()
				.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
				.setMaster("local[2]")
			// 1.b 传递SparkConf对象，构建Context实例
			new SparkContext(sparkConf)
		}
		
		// 1、并行化集合创建RDD数据集
		val linesSeq: Seq[String] = Seq(
			"hadoop scala hive spark scala sql sql", //
			"hadoop scala spark hdfs hive spark", //
			"spark hdfs spark hdfs scala hive spark" //
		)
		val inputRDD: RDD[String] = sc.parallelize(linesSeq, numSlices = 2)
		
		// 2、分割单词，转换为二元组
		val wordsRDD: RDD[(String, Int)] = inputRDD
			.flatMap(line => line.split("\\s+"))
			.map(word => word -> 1)
		
		// TODO: 先使用groupByKey函数分组，再使用map函数聚合
		val wordsGroupRDD: RDD[(String, Iterable[Int])] = wordsRDD.groupByKey()
		val resultRDD: RDD[(String, Int)] = wordsGroupRDD.map{ case (word, values) =>
			val count: Int = values.sum
			word -> count
		}
		println(resultRDD.collectAsMap())
		
		// TODO: 直接使用reduceByKey或foldByKey分组聚合
		val resultRDD2: RDD[(String, Int)] = wordsRDD.reduceByKey((tmp, item) => tmp + item)
		println(resultRDD2.collectAsMap())
		val resultRDD3 = wordsRDD.foldByKey(0)((tmp, item) => tmp + item)
		println(resultRDD3.collectAsMap())
		
		
		// TODO: 使用aggregateByKey聚合
		/*
		def aggregateByKey[U: ClassTag]
		(zeroValue: U) // 聚合中间临时变量初始值，类似fold函数zeroValue
		(
			seqOp: (U, V) => U, // 各个分区内数据聚合操作函数
			combOp: (U, U) => U // 分区间聚合结果的聚合操作函数
		): RDD[(K, U)]
		*/
		val resultRDD4 = wordsRDD.aggregateByKey(0)(
			(tmp: Int, item: Int) => {
				tmp + item
			},
			(tmp: Int, result: Int) => {
				tmp + result
			}
		)
		
		println(resultRDD4.collectAsMap())
		
		// 应用程序运行结束，关闭资源
		Thread.sleep(1000000)
		sc.stop()
	}
	
}
