package cn.itcast.spark.work

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 基于IDEA集成开发环境，编程实现从TCP Socket实时读取流式数据，对每批次中数据进行词频统计。
 */
object StreamingWork02 {
	
	def main(args: Array[String]): Unit = {
		
		// TODO: 1. 创建StreamingContext流式上下文对象，传递时间间隔
		val ssc: StreamingContext = {
			// a. 创建SparkConf对象，设置应用属性，比如名称和master
			val sparkConf: SparkConf = new SparkConf()
    			.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    			.setMaster("local[3]") // 启动3个Thread线程
			// b. 设置批处理时间间隔BatchInterval：5秒
			new StreamingContext(sparkConf, Seconds(5))
		}
		
		// 2. 从TCP Socket读取流式数据
		/*
		  def socketTextStream(
		      hostname: String,
		      port: Int,
		      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
		    ): ReceiverInputDStream[String]
		 */
		val inputDStream: DStream[String] = ssc.socketTextStream(
			"node1.itcast.cn",
			9999,//
			// TODO： Receiver接收数据以后，按照时间间隔 划分流式数据：blockInterval，默认值为：200ms，可以设置存储级别
			storageLevel = StorageLevel.MEMORY_AND_DISK
		)
		
		// 3. 实时分析数据：对每批次数据进行词频统计Wordcount
		val resultDStream: DStream[(String, Int)] = inputDStream
			// 过滤数据
			.filter(line => null != line && line.trim.length > 0)
			// 对每条数据进行分割为单词
			.flatMap(line => line.trim.split("\\s+"))
			// 转换为二元组，表示每个单词出现一次
			.map(word => word -> 1)
			// 按照单词word分组，进行组内聚合
			.reduceByKey(_ + _)
			
		// 4. 将每批次处理结果打印控制台
		resultDStream.print()
		
		// TODO: 5. 流式应用需要显示启动执行
		ssc.start()  // 只要启动流式应用，一直运行，除非认为终止和程序异常结束
		ssc.awaitTermination()
		// 设置关闭SparkContext和优雅关闭（关闭时，如果正在处理某一个批次数据，处理完成再关闭）
		ssc.stop(stopSparkContext = true, stopGracefully = true)
	}
	
}
