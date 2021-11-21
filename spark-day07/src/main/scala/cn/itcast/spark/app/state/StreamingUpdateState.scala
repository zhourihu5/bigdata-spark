package cn.itcast.spark.app.state

import cn.itcast.spark.app.StreamingContextUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * 实时消费Kafka Topic数据，累加统计各个搜索词的搜索次数，实现百度搜索风云榜
 */
object StreamingUpdateState {
	
	def main(args: Array[String]): Unit = {
		// 1. 获取StreamingContext实例对象
		val ssc: StreamingContext = StreamingContextUtils.getStreamingContext(this.getClass, 5)
		
		// TODO: 设置检查点目录，存储以前的状态数据
		ssc.checkpoint(s"datas/searchlogs/state-1001-${System.nanoTime()}")
		
		// 2. 从Kafka消费数据，使用Kafka New Consumer API
		val kafkaDStream: DStream[ConsumerRecord[String, String]] = StreamingContextUtils
			.consumerKafka(ssc, "search-log-topic")
		
		
		// 3. 对每批次的数据进行搜索词次数统计
		val reduceDStream: DStream[(String, Int)] = kafkaDStream.transform{ rdd =>
			// TODO： 数据格式 -> 9ff75446e7822928,106.90.139.4,20200919174620234,美国纽约州发生枪击案
			val reduceRDD: RDD[(String, Int)] = rdd
    			// 获取Topic中每条数据Messsage
    			.map(record => record.value())
				// 过滤数据
    			.filter(msg => null != msg && msg.trim.split(",").length == 4)
				// 提取：搜索词，以二元组形式表示被搜索一次
    			.map{msg =>
				    val keyword = msg.trim.split(",")(3)
				    (keyword, 1)
			    }
				// 按照搜索词分组，聚合：表示当前批次中数据各个搜索词被搜索的次数
    			.reduceByKey((tmp, item) => tmp + item)
			// 返回每批次数据聚合结果
			reduceRDD
		}
		
		// 4. 调用DStream中updateStateByKey状态更新函数，将当前批次状态与以前状态合并更新
		/*
		  def updateStateByKey[S: ClassTag](
		      updateFunc: (Seq[V], Option[S]) => Option[S]
		    ): DStream[(K, S)]
		    参数一：Seq[V]
		         当前批次中Key对应的所有Value的集合，由于前面已对每批次数据按照Key聚合，所以此时Seq只有一个值
		    参数二：Option[S]
		         表示当前Key的以前状态，可能此Key在以前没有出现过，所有无状态：None，如果出现过：Some
		         其中S表示泛型，表示Key的状态类型，针对当前应用来说，类型Int类型：次数
		 */
		val stateDStream: DStream[(String, Int)] = reduceDStream.updateStateByKey(
			(values: Seq[Int], state: Option[Int]) => {
				// 1. 获取当前批次中Key的状态值
				val currentState: Int = values.sum
				// 2. 获取Key的以前状态值
				val previousState: Int = state.getOrElse(0)
				// 3. 合并状态
				val latestState: Int = currentState + previousState
				// 4. 返回最新状态
				Some(latestState)
			}
		)
		
		// 5. 将结果数据输出 -> 将每批次的数据处理以后输出
		stateDStream.print()
		
		// 6.启动流式应用，一直运行，直到程序手动关闭或异常终止
		ssc.start()
		ssc.awaitTermination()
		ssc.stop(stopSparkContext = true, stopGracefully = true)
	}
	
}
