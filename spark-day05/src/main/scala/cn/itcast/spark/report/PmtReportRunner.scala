package cn.itcast.spark.report

import cn.itcast.spark.utils.SparkUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel


/**
 * 针对广告点击数据，依据需求进行报表开发，具体说明如下：
 *- 各地域分布统计：region_stat_analysis
 *- 广告区域统计：ads_region_analysis
 *- 广告APP统计：ads_app_analysis
 *- 广告设备统计：ads_device_analysis
 *- 广告网络类型统计：ads_network_analysis
 *- 广告运营商统计：ads_isp_analysis
 *- 广告渠道统计：ads_channel_analysis
 */
object PmtReportRunner {
	
	def main(args: Array[String]): Unit = {
		
		// 设置Spark应用程序运行的用户：root, 默认情况下为当前系统用户
		System.setProperty("user.name", "root")
		System.setProperty("HADOOP_USER_NAME", "root")
		
		// 1. 创建SparkSession实例对象
		val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
		import spark.implicits._
		
		// 2. 从Hive表中加载广告ETL数据，日期过滤
		val pmtDF: Dataset[Row] = spark.read
			.format("hive")
    		.table("itcast_ads.pmt_ads_info")
			// where date_str = '2020-09-15'
    		.where($"date_str".equalTo(date_sub(current_date(), 1).cast(StringType)))
		//etlDF.show(10, truncate = false)
		
		// TODO: 判断是否有数据，如果没有数据，结束程序，不需要再进行报表分析
		if(pmtDF.isEmpty) System.exit(-1)
		
		// 由于数据被使用多次，所以建议缓存
		pmtDF.persist(StorageLevel.MEMORY_AND_DISK)
		
		// 3. 依据不同业务需求开发报表
		// 3.1. 地域分布统计：region_stat_analysis
		RegionStateReport.doReport(pmtDF)
		// 3.2. 广告区域统计：ads_region_analysis
		AdsRegionAnalysisReport.doReport(pmtDF)
		// 3.3. 广告APP统计：ads_app_analysis
		//AdsAppAnalysisReport.processData(pmtDF)
		// 3.4. 广告设备统计：ads_device_analysis
		//AdsDeviceAnalysisReport.processData(pmtDF)
		// 3.5. 广告网络类型统计：ads_network_analysis
		//AdsNetworkAnalysisReport.processData(pmtDF)
		// 3.6. 广告运营商统计：ads_isp_analysis
		//AdsIspAnalysisReport.processData(pmtDF)
		// 3.7. 广告渠道统计：ads_channel_analysis
		//AdsChannelAnalysisReport.doReport(pmtDF)
		
		// 释放资源
		pmtDF.unpersist()
		
		// 4. 应用结束，关闭资源
		Thread.sleep(10000000)
		spark.stop()
	}
	
}
