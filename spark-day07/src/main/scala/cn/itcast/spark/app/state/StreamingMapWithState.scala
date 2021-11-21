package cn.itcast.spark.app.state

import cn.itcast.spark.app.StreamingContextUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{State, StateSpec, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
 * 实时消费Kafka Topic数据，累加统计各个搜索词的搜索次数，实现百度搜索风云榜
 */
object StreamingMapWithState {
	
	def main(args: Array[String]): Unit = {
		
		// 1. 获取StreamingContext实例对象
		val ssc: StreamingContext = StreamingContextUtils.getStreamingContext(this.getClass, 5)
		// TODO: 设置检查点目录
		ssc.checkpoint(s"datas/streaming/state-${System.nanoTime()}")
		
		// 2. 从Kafka消费数据，使用Kafka New Consumer API
		val kafkaDStream: DStream[ConsumerRecord[String, String]] = StreamingContextUtils
			.consumerKafka(ssc, "search-log-topic")
		
		// 3. 对每批次的数据进行搜索词进行次数统计
		val reduceDStream: DStream[(String, Int)] = kafkaDStream.transform{ rdd =>
			val reduceRDD: RDD[(String, Int)] = rdd
				// 过滤不合格的数据
				.filter{ record =>
					val message: String = record.value()
					null != message && message.trim.split(",").length == 4
				}
				// 提取搜索词，转换数据为二元组，表示每个搜索词出现一次
				.map{record =>
					val keyword: String = record.value().trim.split(",").last
					keyword -> 1
				}
				// 按照单词分组，聚合统计
				.reduceByKey((tmp, item) => tmp + item) // TODO: 先聚合，再更新，优化
			// 返回
			reduceRDD
		}
		
		// TODO: 4、实时累加统计搜索词搜索次数，使用mapWithState函数
		/*
		  def mapWithState[StateType: ClassTag, MappedType: ClassTag](
		      spec: StateSpec[K, V, StateType, MappedType]
		    ): MapWithStateDStream[K, V, StateType, MappedType]
		 */
		// 状态更新函数，针对每条数据进行更新状态
		val spec: StateSpec[String, Int, Int, (String, Int)] = StateSpec.function(
			// (KeyType, Option[ValueType], State[StateType]) => MappedType
			// TODO: mappingFunction: (KeyType, Option[ValueType], State[StateType]) => MappedType
			(keyword: String, countOption: Option[Int], state: State[Int]) => {
				// a. 获取当前批次中搜索词搜索次数
				val currentState: Int = countOption.getOrElse(0)
				// b. 从以前状态中获取搜索词搜索次数
				val previousState = state.getOption().getOrElse(0)
				// c. 搜索词总的搜索次数
				val latestState = currentState + previousState
				// d. 更行状态
				state.update(latestState)
				// e. 返回最新搜索次数
				(keyword, latestState)
			}
		)
		// 调用mapWithState函数进行实时累加状态统计
		val stateDStream: DStream[(String, Int)] = reduceDStream.mapWithState(spec)
		
		// 5. 将结果数据输出 -> 将每批次的数据处理以后输出
		stateDStream.print()
		
		// 6.启动流式应用，一直运行，直到程序手动关闭或异常终止
		ssc.start()
		ssc.awaitTermination()
		ssc.stop(stopSparkContext = true, stopGracefully = true)
	}
	
}
