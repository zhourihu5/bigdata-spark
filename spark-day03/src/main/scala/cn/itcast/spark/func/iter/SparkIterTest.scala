package cn.itcast.spark.func.iter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
 * 使用spark实现词频统计WordCount，此处使用Scala语言编写
 */
object SparkIterTest {
	
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
		
		// 2. 处理分析数，调用RDD中Transformation函数
		val resultRDD: RDD[(String, Int)] = inputRDD
			// 过滤空数据
			.filter(line => null != line && line.trim.length != 0)
			// 每行数据分割单词
			.flatMap(line => line.trim.split("\\s+"))
			// TODO: 针对分区进行操作，转换为二元组，表示每个单词出一次
    		.mapPartitions{iter =>
			    //val xx: Iterator[String] = iter
			    iter.map(word => word -> 1)
		    }
			// 分组聚合
			.reduceByKey((tmp, item) => tmp + item)
		
		// 3. 结果数据输出, 调用Action函数
		resultRDD
			//TODO: 针对分区操作，将结果数据打印
            .foreachPartition{iter =>
	            // 获取分区编号
	            val partitionId: Int = TaskContext.getPartitionId()
	            
			    //val xx: Iterator[(String, Int)] = iter
	            iter.foreach(tuple => println(s"${partitionId}: $tuple"))
		    }
			//.foreach(tuple => println(tuple))
		
		// 应用结束，关闭资源
		sc.stop()
	}
	
}
