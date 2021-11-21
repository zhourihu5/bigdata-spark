package cn.itcast.spark.kafka

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 基于IDEA集成开发环境，编程实现从Kafka Topic实时读取流式数据，对每批次中数据进行词频统计。
 *      TODO: 采用 Kafka Old Simple Level Consumer API  -> Direct 方式
 */
object StreamingKafkaDirect {
	
	def main(args: Array[String]): Unit = {
		
		// TODO: 1. 创建StreamingContext流式上下文对象，传递时间间隔
		val ssc: StreamingContext = {
			// a. 创建SparkConf对象，设置应用属性，比如名称和master
			val sparkConf: SparkConf = new SparkConf()
    			.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    			.setMaster("local[3]") // 启动3个Thread线程
				// TODO: 设置每批次RDD中各个分区数据的最大值 -> 每个分区每秒的最大数据量
    			.set("spark.streaming.kafka.maxRatePerPartition", "10000")
			// b. 设置批处理时间间隔BatchInterval：5秒
			new StreamingContext(sparkConf, Seconds(5))
		}
		
		// 2. 采用Old Consume API中Direct方式消费Kafka Topic数据
		/*
		  def createDirectStream[
		    K: ClassTag,
		    V: ClassTag,
		    KD <: Decoder[K]: ClassTag,
		    VD <: Decoder[V]: ClassTag] (
		      ssc: StreamingContext,
		      kafkaParams: Map[String, String],
		      topics: Set[String]
		  ): InputDStream[(K, V)]
		 */
		// a. 从Kafka 消费数据时参数设置
		val kafkaParams: Map[String, String] = Map(
			"bootstrap.servers" -> "node1.itcast.cn:9092", //
			"auto.offset.reset" -> "largest"
		)
		// b. 消费Topic名
		val topics: Set[String] = Set("wc-topic")
		// c. 采用Direct方式消费数据
		val kafkaDStream: DStream[(String, String)] = KafkaUtils.createDirectStream[
			String, String, StringDecoder, StringDecoder](
			ssc, //
			kafkaParams, //
			topics
		)
		// TODO: 表示从Kafka消费数据以后，仅仅获取Value的值: Message
		val inputDStream: DStream[String] = kafkaDStream.map(tuple => tuple._2)
		
		// 3. 实时分析数据：对每批次数据进行词频统计Wordcount
		// TODO: SparkStreaming实际应用开发中，建议使用transform函数，针对每批次RDD转换操作
		/*
			def transform[U: ClassTag](transformFunc: RDD[T] => RDD[U]): DStream[U]
		 */
		val resultDStream: DStream[(String, Int)] =inputDStream.transform{rdd =>
			// TODO: 此时参数rdd表示每批次RDD数据
			val resultRDD: RDD[(String, Int)] = rdd
				// 过滤数据
				.filter(line => null != line && line.trim.length > 0)
				// 对每条数据进行分割为单词
				.flatMap(line => line.trim.split("\\s+"))
				// 转换为二元组，表示每个单词出现一次
				.map(word => word -> 1)
				// 按照单词word分组，进行组内聚合
				.reduceByKey(_ + _)
			// 返回转换的RDD
			resultRDD
		}
		
		// 4. 将每批次处理结果打印控制台
		//resultDStream.print()
		// TODO： 针对每批次结果RDD进行输出操作
		/*
		def foreachRDD(foreachFunc: (RDD[T], Time) => Unit): Unit
		 */
		resultDStream.foreachRDD{(resultRDD, batchTime) =>
			// 将batchTime进行转换：yyyy-MM-dd HH:mm:ss
			val formatTime = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
				.format(batchTime.milliseconds)
			println("-------------------------------------------")
			println(s"Time: $formatTime")
			println("-------------------------------------------")
			// 判断结果RDD是否有数据，没有数据不要打印
			if(!resultRDD.isEmpty()){
				// TODO: 针对RDD数据进行输出，以前在SparkCore中怎么编写此处就编写
				resultRDD.coalesce(1).foreachPartition(iter => iter.foreach(println))
			}
		}
		
		// TODO: 5. 流式应用需要显示启动执行
		ssc.start()  // 只要启动流式应用，一直运行，除非认为终止和程序异常结束
		ssc.awaitTermination()
		// 设置关闭SparkContext和优雅关闭（关闭时，如果正在处理某一个批次数据，处理完成再关闭）
		ssc.stop(stopSparkContext = true, stopGracefully = true)
	}
	
}
