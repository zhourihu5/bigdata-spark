package cn.itcast.spark.ckpt

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD数据Checkpoint设置，案例演示
 */
object SparkCkptTest {
	
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
		
		// TODO: 第一步、设置Checkpoint 检查点目录
		sc.setCheckpointDir("datas/ckpt/1001/")
		
		// 加载数据
		val datasRDD: RDD[String] = sc.textFile("datas/wordcount.data")
		
		// TODO： 第二步、将RDD Checkpoint操作
		datasRDD.checkpoint()
		datasRDD.count() // 触发执行
		
		// 执行Action函数，此时从哪里读取数据
		println(s"count = ${datasRDD.count()}")
		
		// 应用程序运行结束，关闭资源
		Thread.sleep(100000)
		sc.stop()
	}
	
}
