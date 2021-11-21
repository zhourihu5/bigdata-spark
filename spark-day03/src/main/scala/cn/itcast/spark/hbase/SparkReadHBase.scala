package cn.itcast.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * SparkCore从HBase表中加载数据，封装至RDD
 */
object SparkReadHBase {
	
	def main(args: Array[String]): Unit = {
		
		// 构建Spark Application应用层入口实例对象
		val sc: SparkContext = {
			// a. 创建SparkConf对象，设置应用信息
			val sparkConf: SparkConf = new SparkConf()
    			.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    			.setMaster("local[2]")
				// TODO: 设置使用Kryo 序列化方式
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				// TODO: 注册序列化的数据类型
				.registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result]))
			// b. 传递SparkConf对象，创建实例
			SparkContext.getOrCreate(sparkConf)
		}
		
		/*
		  def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
		      conf: Configuration = hadoopConfiguration,
		      fClass: Class[F],
		      kClass: Class[K],
		      vClass: Class[V]
		  ): RDD[(K, V)]
		 */
		// i. 连接HBase时Client配置信息
		val conf: Configuration = HBaseConfiguration.create()
		// TODO: 连接HBase表Zookeeper相关信息
		conf.set("hbase.zookeeper.quorum", "node1.itcast.cn")
		conf.set("hbase.zookeeper.property.clientPort", "2181")
		conf.set("zookeeper.znode.parent", "/hbase")
		// TODO: 表的名称
		conf.set(TableInputFormat.INPUT_TABLE, "htb_wordcount")
		
		// ii. 调用底层TableInputFormat加载数据
		val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc
			.newAPIHadoopRDD[ImmutableBytesWritable, Result, TableInputFormat](
				conf, //
				classOf[TableInputFormat], //
				classOf[ImmutableBytesWritable], //
				classOf[Result] //
			)
		println(s"Count = ${hbaseRDD.count()}")
		
		/*
			java.io.NotSerializableException: org.apache.hadoop.hbase.io.ImmutableBytesWritable
			分析：表示ImmutableBytesWritable不能被序列化
			解决：设置序列化为Kryo方式即可
		 */
		hbaseRDD
			.take(5) // 产生网络传输，使得需要对数据进行序列化操作
			.foreach{case (rowKey, result) =>
				println(s"RowKey = ${Bytes.toString(rowKey.get())}")
				result.rawCells().foreach(cell => {
					val cf = Bytes.toString(CellUtil.cloneFamily(cell))
					val column = Bytes.toString(CellUtil.cloneQualifier(cell))
					val value = Bytes.toString(CellUtil.cloneValue(cell))
					println(s"\t$cf:$column = $value")
				})
			}
		
		// 应用结束，关闭资源
		sc.stop()
	}
	
}
