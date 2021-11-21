package cn.itcast.spark.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

/**
 * SparkSQL集成Hive，读取数据，自定义UDF函数，两种方式：DSL和SQL
 */
object SparkSQLUdf {
	
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
		
		// TODO: 将某个字段转换为小写
		val to_lower: UserDefinedFunction = udf(
			(name: String) => {
				name.toLowerCase
			}
		)
		
		// 2. 采用DSL方式读取Hive表数据
		spark.read
			.table("db_hive.emp")
    		.select(
			    $"ename",
			    // 调用自定义函数
			    to_lower($"ename").as("name"),
			    // 添加一列
			    lit("xx").as("xx_name")
		    )
			.show(20, truncate = false)
		
		println("=================================================")
		
		// 自定义UDF函数，在SQL中使用
		spark.udf.register(
			"self_to_lower", // 函数名称
			(name: String) => {     // 函数体
				name.toLowerCase
			}
		)
		
		// 3. 采用SQL方式分析数据
		spark.sql(
			"""
			  |SELECT ename, self_to_lower(ename) AS name  FROM db_hive.emp
			  |""".stripMargin)
			.show(20, truncate = false)
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
