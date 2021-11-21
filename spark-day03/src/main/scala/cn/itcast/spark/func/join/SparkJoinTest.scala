package cn.itcast.spark.func.join

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD中关联函数Join，针对RDD中数据类型为Key/Value对
 */
object SparkJoinTest {
	
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
		sc.setLogLevel("WARN")
		
		
		// TODO: 模拟数据集
		val empRDD: RDD[(Int, String)] = sc.parallelize(
			Seq((101, "zhangsan"), (102, "lisi"), (103, "wangwu"), (104, "zhangliu"))
		)
		val deptRDD: RDD[(Int, String)] = sc.parallelize(
			Seq((101, "sales"), (102, "tech"))
		)
		
		// TODO: 等值JOIN
		val joinRDD: RDD[(Int, (String, String))] = empRDD.join(deptRDD)
		joinRDD.foreach{case(deptNo, (empName, deptName)) =>
			println(s"deptNo = $deptNo, empName = $empName, deptName = $deptName")
		}
		
		// TODO：左外连接
		val leftJoinRDD: RDD[(Int, (String, Option[String]))] = empRDD.leftOuterJoin(deptRDD)
		leftJoinRDD.foreach{case(deptNo, (empName, option)) =>
			val deptName: String = option.getOrElse("no")
			println(s"deptNo = $deptNo, empName = $empName, deptName = $deptName")
		}
		
		// 应用程序运行结束，关闭资源
		sc.stop()
	}
	
}
