package cn.itcast.spark.etl

import cn.itcast.spark.config.ApplicationConfig
import cn.itcast.spark.utils.{IpUtils, SparkUtils}
import org.apache.spark.SparkFiles
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

/**
 * 广告数据进行ETL处理，具体步骤如下：
 *      第一步、加载json数据
 *      第二步、解析IP地址为省份和城市
 *      第三步、数据保存至Hive表
 */
object PmtEtlRunner extends Logging{
	
	/**
	 * 对数据进行ETL处理，调用ip2Region第三方库，解析IP地址为省份和城市
	 */
	def processData(dataframe: DataFrame): DataFrame = {
		// 从DataFrame中获取SparkSession实例对象
		val session: SparkSession = dataframe.sparkSession
		
		// TODO： 将数据字典文件进行分布式缓存
		session.sparkContext.addFile(ApplicationConfig.IPS_DATA_REGION_PATH)
		
		// 1. 针对每个分区数据操作，转换IP地址为省份和城市
		val rowsRDD: RDD[Row] = dataframe.rdd.mapPartitions{ iter =>
			//val xx: Iterator[Row] = iter
			// 创建DbSearch对象，传递字典数据, 每个分区创建一个
			val dbSearcher: DbSearcher = new DbSearcher(new DbConfig(), SparkFiles.get("ip2region.db"))
			
			iter.map{row =>
				// 提取IP地址的值
				val ipValue = row.getAs[String]("ip")
				// 解析IP 地址
				val region: Region = IpUtils.convertIpToRegion(ipValue, dbSearcher)
				// 返回Row对象
				val newSeq: Seq[Any] = row.toSeq :+ region.province :+ region.city
				// 返回Row对象
				Row.fromSeq(newSeq)
			}
		}
		
		// 2. 获取Schema信息
		val newSchema: StructType = dataframe.schema
			.add("province", StringType, nullable = true)
			.add("city", StringType, nullable = true)
		
		// 3. 创建新的DataFrame
		val df: DataFrame = session.createDataFrame(rowsRDD, newSchema)
		
		// 4. 添加一个字段，表示数据是哪一天，保存Hive分区表时，使用此字段
		df.withColumn("date_str", date_sub(current_date(), 1).cast(StringType))
	}
	
	/**
	 * 将DataFrame数据保存Parquet文件中
	 */
	def saveAsParquet(dataframe: DataFrame): Unit = {
		/*
		分区目录结构：
			dataset/pmt-etl
			   /date_str=2020-09-15
			   /date_str=2020-09-16
			   /date_str=2020-09-17
		 */
		dataframe.write
    		.mode(SaveMode.Overwrite)
    		.partitionBy("date_str")
    		.parquet("dataset/pmt-etl")
	}
	
	/**
	 * 保存数据至Hive分区表中，按照日期字段分区
	 */
	def saveAsHiveTable(dataframe: DataFrame): Unit = {
		dataframe
			.coalesce(1) // 降低DataFrame分区数目，数据文件就1个
			.write
			.mode(SaveMode.Append)
			.format("hive") // 一定要指定为hive数据源，否则报错
			.partitionBy("date_str")
			.saveAsTable("itcast_ads.pmt_ads_info")
	}
	
	
	def main(args: Array[String]): Unit = {
		
		// 设置Spark应用程序运行的用户：root, 默认情况下为当前系统用户
		System.setProperty("user.name", "root")
		System.setProperty("HADOOP_USER_NAME", "root")
		
		// TODO: 1. 创建SparkSession对象, 导入隐式转换
		val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
		import spark.implicits._
		
		// TODO: 2. 从文件系统加载业务数据
		val pmtDF: DataFrame = spark.read.json(ApplicationConfig.DATAS_PATH)
		//pmtDF.printSchema()
		//pmtDF.show(10, truncate = false)
		
		// TODO: 3. 解析IP地址为省份和城市
		val etlDF: DataFrame = processData(pmtDF)
		//etlDF.select($"ip", $"province", $"city", $"date_str").show(50, truncate = false)
		
		// TODO: 4. 保存ETL后数据至Hive分区表中
		// 保存数据为parquet格式文件，并且按照date_str分区保存
		//saveAsParquet(etlDF)
		saveAsHiveTable(etlDF)
		
		// 应用结束，关闭资源
		//Thread.sleep(1000000)
		spark.stop()
	}
	
}
