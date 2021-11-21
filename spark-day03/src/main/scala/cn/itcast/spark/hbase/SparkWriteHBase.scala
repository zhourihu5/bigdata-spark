package cn.itcast.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将RDD数据保存至HBase表中
 */
object SparkWriteHBase {
	
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
			// 转换为二元组，表示每个单词出一次
			.map(word => word -> 1)
			// 分组聚合
			.reduceByKey((tmp, item) => tmp + item)
		
		// TODO: 将结果RDD保存至HBase表中
		/*
			表名称：htb_wordcount
			RowKey：word
			ColumnFamily: info
			Columns: count
			创建表语句：
					create 'htb_wordcount', 'info'
		 */
		// TODO： 第一步、将结果RDD转换格式RDD[(ImmutableBytesWritable: RowKey, Put)]
		// val resultRDD: RDD[(String, Int)]
		val putsRDD: RDD[(ImmutableBytesWritable, Put)] = resultRDD.map{case(word, count) =>
			// 创建RowKey
			val rowKey = new ImmutableBytesWritable(Bytes.toBytes(word))
			// 创建Put对象
			val put: Put = new Put(rowKey.get())
			// 添加列Column
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("count"), Bytes.toBytes(count.toString))
			// 返回二元组
			(rowKey, put)
		}
		
		// TODO: 第二步、保存数据至HBase表
		/*
		  def saveAsNewAPIHadoopFile(
		      path: String,
		      keyClass: Class[_],
		      valueClass: Class[_],
		      outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
		      conf: Configuration = self.context.hadoopConfiguration
		  ): Unit
		 */
		// i. 连接HBase时Client配置信息
		val conf: Configuration = HBaseConfiguration.create()
		// TODO: 连接HBase表Zookeeper相关信息
		conf.set("hbase.zookeeper.quorum", "node1.itcast.cn")
		conf.set("hbase.zookeeper.property.clientPort", "2181")
		conf.set("zookeeper.znode.parent", "/hbase")
		// TODO: 表的名称
		conf.set(TableOutputFormat.OUTPUT_TABLE, "htb_wordcount")
		// ii. 调用TableOutputFormat保存数据
		putsRDD.saveAsNewAPIHadoopFile(
			"datas/hbase/wc-1001", //
			classOf[ImmutableBytesWritable], //
			classOf[Put], //
			classOf[TableOutputFormat[ImmutableBytesWritable]], //
			conf
		)
		
		// 应用结束，关闭资源
		sc.stop()
	}
	
}
