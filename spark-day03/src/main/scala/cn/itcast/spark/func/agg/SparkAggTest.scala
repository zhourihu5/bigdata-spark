package cn.itcast.spark.func.agg

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

import scala.collection.mutable.ListBuffer

/**
 * 针对RDD中聚合函数，如何进行使用及底层原理
 *      reduce/fold
 *      aggregate
 *      groupByKey/reduceByKey/foldByKey/aggregateByKey/combineByKey
 */
object SparkAggTest {
	
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
		
		// TODO:  RDD 中reduce和fold函数
		val datasRDD: RDD[Int] = sc.parallelize(1 to 10, numSlices = 2)
		datasRDD.foreachPartition{iter =>
			val partitionId: Int = TaskContext.getPartitionId()
			iter.foreach(item => println(s"$partitionId: $item"))
		}
		
		println("===============================================")
		
		// 使用reduce函数
		val result: Int = datasRDD
			.reduce{(tmp, item) =>
				val partitionId: Int = TaskContext.getPartitionId()
				println(s"$partitionId： tmp = $tmp, item = $item, sum = ${tmp + item}")
				tmp + item
			}
		println(s"RDD Reduce = $result")
		
		println("===============================================")
		
		// 使用aggregate函数实现RDD中最大的两个数据
		/*
		def aggregate[U: ClassTag]
			// TODO： 表示聚合函数中间临时变量初始值
			(zeroValue: U)
			(
				// TODO: 分区内数据聚合时使用聚合函数
				seqOp: (U, T) => U,
				// TODO: 分区间聚合数据聚合时使用聚合函数
				combOp: (U, U) => U
			): U
		 */
		val rest: ListBuffer[Int] = datasRDD.aggregate(new ListBuffer[Int]())(
			// 分区内聚合函数 seqOp: (U, T) => U
			(tmp: ListBuffer[Int], item: Int) => {
				tmp += item
				tmp.sorted.takeRight(2)
			},
			// 分区间聚合函数 combOp: (U, U) => U
			(tmp: ListBuffer[Int], item: ListBuffer[Int]) => {
				tmp ++= item
				tmp.sorted.takeRight(2)
			}
		)
		println(s"Top2: ${rest.toList.mkString(", ")}")
		
		
		
		// 应用结束，关闭资源
		sc.stop()
	}
	
}
