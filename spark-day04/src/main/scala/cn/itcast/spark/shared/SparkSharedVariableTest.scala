package cn.itcast.spark.shared

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用spark实现词频统计WordCount，此处使用Scala语言编写
 */
object SparkSharedVariableTest {
	
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
		val inputRDD: RDD[String] = sc.textFile("datas/filter/datas.input", minPartitions = 2)
		
		// TODO: 字典数据，只要有这些单词就过滤: 特殊字符存储列表List中
		val list: List[String] = List(",", ".", "!", "#", "$", "%")
		// TODO：将变量数据广播出去,将变量数据返送给所有的Executor
		val broadcastList: Broadcast[List[String]] = sc.broadcast(list)
		
		// TODO: 定义累加器，记录单词为符号数据的个数
		val accumulator: LongAccumulator = sc.longAccumulator("number_accum")
		
		// 2. 处理分析数，调用RDD中Transformation函数
		val resultRDD: RDD[(String, Int)] = inputRDD
			// 过滤空数据
			.filter(line => null != line && line.trim.length != 0)
			// 每行数据分割单词
			.flatMap(line => line.trim.split("\\s+"))
			// TODO: 过滤非单词字符
    		.filter{word =>
			    // TODO: 获取广播变量的值
			    val listValue: List[String] = broadcastList.value
			    
			    val isFlag = listValue.contains(word)
			    if(isFlag){
				    accumulator.add(1L) // 累加器加1操作
			    }
			    // 返回值
			   !isFlag
		    }
			// 转换为二元组，表示每个单词出一次
			.map(word => word -> 1)
			// 分组聚合
			.reduceByKey((tmp, item) => tmp + item)
		
		// 3. 结果数据输出, 调用Action函数
		resultRDD.foreach(tuple => println(tuple))
		
		// TODO: 获取累加器的值, 需要Action函数触发
		println(s"number_accum = ${accumulator.value}")
		
		// 应用结束，关闭资源
		Thread.sleep(1000000)
		sc.stop()
	}
	
}
