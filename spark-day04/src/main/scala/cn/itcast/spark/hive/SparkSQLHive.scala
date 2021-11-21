package cn.itcast.spark.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * SparkSQL集成Hive，读取数据，进行分析处理
 */
object SparkSQLHive {
	
	def main(args: Array[String]): Unit = {
		
		// 1. 创建SparkSession对象
		val spark: SparkSession = SparkSession.builder()
    		.appName(this.getClass.getSimpleName.stripSuffix("$"))
    		.master("local[4]")
			// TODO： 集成Hive，配置MetaStore地址信息
    		.config("hive.metastore.uris", "thrift://node1.itcast.cn:9083")
    		.enableHiveSupport() // 表示集成Hive，显示指定集成
    		.getOrCreate()
		import spark.implicits._
		
		// 2. 采用DSL方式读取Hive表数据
		spark.read
			.table("db_hive.emp")
    		// 统计各个部门平均工资
			.groupBy($"deptno")
			.agg(
				round(avg($"sal"), 2).as("avg_sal")
			)
			.show(4, truncate = false)
		
		println("=================================================")
		
		// 3. 采用SQL方式分析数据
		spark.sql(
			"""
			  |SELECT deptno, ROUND(AVG(sal), 2) AS avg_sal FROM db_hive.emp GROUP BY deptno
			  |""".stripMargin)
			.show(4, truncate = false)
		
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
