package cn.itcast.spark.offset

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 集成Kafka，实时消费Topic中数据，获取每批次数据对应Topic各个分区数据偏移量
 */
object StreamingKafkaOffset {
	
	def main(args: Array[String]): Unit = {
		
		// 1. 创建StreamingContext流式上下文对象，传递时间间隔
		val ssc: StreamingContext = {
			// a. 创建SparkConf对象，设置应用属性，比如名称和master
			val sparkConf: SparkConf = new SparkConf()
    			.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    			.setMaster("local[3]") // 启动3个Thread线程
				// 设置每批次RDD中各个分区数据的最大值 -> 每个分区每秒的最大数据量
    			.set("spark.streaming.kafka.maxRatePerPartition", "10000")
			// b. 设置批处理时间间隔BatchInterval：5秒
			new StreamingContext(sparkConf, Seconds(5))
		}
		
		// 2. 采用New Consume API消费Kafka Topic数据
		/*
		  def createDirectStream[K, V](
		      ssc: StreamingContext,
		      locationStrategy: LocationStrategy,
		      consumerStrategy: ConsumerStrategy[K, V]
		    ): InputDStream[ConsumerRecord[K, V]]
		 */
		// a. 消费Kakfa数据时：位置策略
		val locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent
		// b. 消费Kafka数据时：消费策略
		/*
		  def Subscribe[K, V](
		      topics: Iterable[jl.String],
		      kafkaParams: collection.Map[String, Object]
		   ): ConsumerStrategy[K, V]
		 */
		// i. 消费Topic名称
		val topics: Iterable[String] = Array("wc-topic")
		// ii. 从Kafka消费数据参数设置
		val kafkaParams: collection.Map[String, Object] = Map[String, Object](
			"bootstrap.servers" -> "node1.itcast.cn:9092",
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> "group-id-1001",
			"auto.offset.reset" -> "latest",
			"enable.auto.commit" -> (false: java.lang.Boolean)
		)
		val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe(
			topics, kafkaParams
		)
		// c. 直接Direct消费数据
		val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils
			.createDirectStream[String, String](
				ssc, //
				locationStrategy, //
				consumerStrategy
			)
		
		// TODO：其一、定义数组存储每批次数据对应RDD中各个分区的Topic Partition中偏移量信息
		var offsetRanges: Array[OffsetRange] = Array.empty
		
		// 3. 实时分析数据：对每批次数据进行词频统计Wordcount
		/*
			def transform[U: ClassTag](transformFunc: RDD[T] => RDD[U]): DStream[U]
		 */
		val resultDStream: DStream[(String, Int)] = kafkaDStream.transform{rdd =>
			// 将rdd（KafkaRDD）子类转换为父类HasOffsetRanges，获取其中偏移量范围
			// TODO：其二、直接从Kafka获取的每批次KafkaRDD中获取偏移量信息
			offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
			
			// 此时参数rdd表示每批次RDD数据
			val resultRDD: RDD[(String, Int)] = rdd
				// 获取Value的值: Message
				.map(record => record.value())
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
				// 针对RDD数据进行输出，以前在SparkCore中怎么编写此处就编写
				resultRDD.coalesce(1).foreachPartition(iter => iter.foreach(println))
			}
			
			// TODO： 其三、将当前批次RDD数据的各个分区偏移量打印
			println("======================== formatTime ========================")
			offsetRanges.foreach{offsetRange =>
				println(s"topic: ${offsetRange.topic}    partition: ${offsetRange.partition}    offsets: ${offsetRange.fromOffset} to ${offsetRange.untilOffset}")
			}
		}
		
		// 5. 流式应用需要显示启动执行
		ssc.start()  // 只要启动流式应用，一直运行，除非认为终止和程序异常结束
		ssc.awaitTermination()
		// 设置关闭SparkContext和优雅关闭（关闭时，如果正在处理某一个批次数据，处理完成再关闭）
		ssc.stop(stopSparkContext = true, stopGracefully = true)
	}
	
}
