package cn.itcast.spark.utils

import cn.itcast.spark.config.ApplicationConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * 构建SparkSession实例对象工具类，加载配置属性
 */
object SparkUtils extends Logging{
	
	/**
	 * 构建SparkSession实例对象
	 * @param clazz 应用程序Class对象
	 * @return SparkSession实例
	 */
	def createSparkSession(clazz: Class[_]): SparkSession = {
		/*
		val spark: SparkSession = SparkSession.builder()
    		.appName(this.getClass.getSimpleName.stripSuffix("$"))
    		
    		.master("local[4]")
        	.config("spark.sql.shuffle.partitons", "4")
    		
			// TODO： 集成Hive，配置MetaStore地址信息
    		.config("hive.metastore.uris", "thrift://node1.itcast.cn:9083")
    		.enableHiveSupport() // 表示集成Hive，显示指定集成
    		
    		.getOrCreate()
		 */
		// 1. 创建SparkSession.Builder对象，设置应用名称
		val builder: SparkSession.Builder = SparkSession
			.builder()
    		.appName(clazz.getSimpleName.stripSuffix("$"))
			// 设置输出文件算法
			.config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
			.config("spark.debug.maxToStringFields", "20000")
		
		// 2. 判断是否是本地模式，如果是设置相关属性值
		if(ApplicationConfig.APP_LOCAL_MODE){
			builder
    			.master(ApplicationConfig.APP_SPARK_MASTER)
				.config("spark.sql.shuffle.partitions", "4")
			logInfo("Spark Application 运行在本地模式........................")
		}
		
		// 3. 判断是否与Hive集成，如果是设置MetaStore URIs地址
		if(ApplicationConfig.APP_IS_HIVE){
			builder
    			.enableHiveSupport()
				.config("hive.metastore.uris", ApplicationConfig.APP_HIVE_META_STORE_URLS)
				.config("hive.exec.dynamic.partition.mode", "nonstrict")
			logWarning("Spark Application与Hive集成.............................")
		}
	
		// 4. 获取SparkSession实例对象
		builder.getOrCreate()
	}
	
}
