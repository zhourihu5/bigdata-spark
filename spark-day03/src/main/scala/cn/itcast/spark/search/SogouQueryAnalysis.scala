package cn.itcast.spark.search

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 用户查询日志(SogouQ)分析，数据来源Sogou搜索引擎部分网页查询需求及用户点击情况的网页查询日志数据集合。
	 * 1. 搜索关键词统计，使用HanLP中文分词
	 * 2. 用户搜索次数统计
	 * 3. 搜索时间段统计
 * 数据格式：
 *      访问时间\t用户ID\t[查询词]\t该URL在返回结果中的排名\t用户点击的顺序号\t用户点击的URL
 *      其中，用户ID是根据用户使用浏览器访问搜索引擎时的Cookie信息自动赋值，即同一次使用浏览器输入的不同查询对应同一个用户ID
 */
object SogouQueryAnalysis {
	
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
		
		// TODO： 1. 加载搜狗搜索日志数据，使用小数据集
		//val sogouRDD: RDD[String] = sc.textFile("datas/sogou/SogouQ.sample", minPartitions = 2)
		val sogouRDD: RDD[String] = sc.textFile("datas/sogou/SogouQ.reduced", minPartitions = 2)
		//println(s"Count = ${sogouRDD.count()}")
		// 00:00:00	2982199073774412	[360安全卫士]	8 3	download.it.com.cn/softweb/software/firewall/antivirus/20067/17938.html
		//println(sogouRDD.first())
		
		// TODO: 2. 数据ETL操作
		val etlRDD: RDD[SogouRecord] = sogouRDD
			.filter(line => null != line && line.trim.split("\\s+").length == 6)
			.mapPartitions{iter =>
				iter.map{line =>
					val array = line.trim.split("\\s+")
					// 构建SogouRecord对象
					SogouRecord(
						array(0), array(1), //
						array(2).replaceAll("\\[|\\]", ""), //
						array(3).toInt, array(4).toInt, array(5)
					)
				}
			}
		//println(etlRDD.first())
		// 由于数据RDD被使用多次，缓存数据
		etlRDD.persist(StorageLevel.MEMORY_AND_DISK)
		
		// TODO: 搜索关键词统计
		val resultRDD: RDD[(String, Int)] = etlRDD
			.filter(record => null != record.queryWords && record.queryWords.trim.length > 0)
			.flatMap{record =>
				// 360安全卫士
				val queryWords = record.queryWords.trim
				
				// 使用HanLP进行中文分词 ->  360, 安全,  卫士
				import java.util
				val terms: util.List[Term] = HanLP.segment(queryWords)
				
				// 封装至二元组，表示每个搜索词出现一次 -> List((360, 1), (安全， 1), (卫士， 1))
				import scala.collection.JavaConverters._
				terms.asScala.map{term => (term.word, 1)}
			}
			// 分组，聚合
			.reduceByKey((tmp, item) => tmp + item)
		// 搜索次数最多的10个搜索词
		resultRDD
			.sortBy(tuple => tuple._2, ascending = false)
			.take(10)
			.foreach(println)
		
		// TODO: 用户搜索点击统计
		/*
			分组字段：先按照用户分组，再按照搜索词分组 -> （userId， queryWords)
		 */
		val preUserQueryWordsCountRDD: RDD[((String, String), Int)] = etlRDD
			.mapPartitions{iter =>
				iter.map{record =>
					val userId = record.userId
					val queryWords = record.queryWords
					// 组合userId和queryWords为Key，表示此用户搜索该词点击一次网页
					((userId, queryWords), 1)
				}
			}
			// 分组，聚合
			.reduceByKey((tmp, item) => tmp + item)
		// TODO： 获取搜索点击次数，最大值，最小值和平均值
		val restRDD: RDD[Int] = preUserQueryWordsCountRDD.values
		println(s"Max Click Count: ${restRDD.max()}")
		println(s"Min Click Count: ${restRDD.min()}")
		println(s"Mean Click Count: ${restRDD.mean()}")
		
		// TODO: 搜索时间段统计 -> 00:00:00 按照小时统计
		etlRDD
			.map{record =>
				// 获取小时
				val hourStr: String = record.queryTime.substring(0, 2)
				// 返回二元组
				(hourStr, 1)
			}
			// 分组，聚合
			.reduceByKey((tmp, item) => tmp + item)
			.top(24)(Ordering.by(tuple => tuple._2))
			.foreach(println)
		
		// 释放资源
		etlRDD.unpersist()
		
		// 应用结束，关闭资源
		Thread.sleep(10000000)
		sc.stop()
	}
	
}
