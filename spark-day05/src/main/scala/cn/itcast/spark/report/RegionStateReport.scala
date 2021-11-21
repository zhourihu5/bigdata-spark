package cn.itcast.spark.report

import java.sql.{Connection, DriverManager, PreparedStatement}

import cn.itcast.spark.config.ApplicationConfig
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
 * 报表开发：按照地域维度（省份和城市）分组统计广告被点击次数
 *      地域分布统计：region_stat_analysis
 */
object RegionStateReport {
	
	/**
	 * 不同业务报表统计分析时，两步骤：
	 * i. 编写SQL或者DSL分析
	 * ii. 将分析结果保存MySQL数据库表中
	 */
	def doReport(dataframe: DataFrame): Unit = {
		val session = dataframe.sparkSession
		import session.implicits._
		
		// i. 报表开发，使用DSL
		val reportDF: Dataset[Row] = dataframe
    		// 按照区域字段分组
    		.groupBy($"province", $"city")
    		.count()
			// 排序
    		.orderBy($"count".desc)
			// 添加日期字段
			.withColumn("report_date", date_sub(current_date(), 1).cast(StringType))
		//reportDF.printSchema()
		//reportDF.show(20, truncate = false)
		
		// ii. 将报表数据保存至MySQL表中
		//saveReportToMySQL(reportDF)
		/*
			此时将报表数据保存MySQL表中，考虑主键是否已经存在，实际业务判断：
				如果主键不存在，直接插入数据；如果主键存在，更新结果
			针对MySQL数据库来说，实现上述功能，两种方式：
			方式一：
				replace into region_stat_analysis(report_date, province, city, count) VALUES('2020-09-15', '上海', '上海市', 9999) ;
			方式二：
				INSERT INTO region_stat_analysis(report_date, province, city, count) VALUES('2020-09-15', '上海', '上海市', 8888) ON DUPLICATE KEY UPDATE count=VALUES(count) ;
		 */
		reportDF
			.coalesce(1)
    		.foreachPartition(iter => saveToMySQL(iter))
	}
	
	/**
	 * 将RDD每个分区数据保存至MySQL表汇总
	 * @param datas 分区数据，封装在Iterator中
	 */
	def saveToMySQL(datas: Iterator[Row]): Unit = {
		// 1. 加载驱动类
		Class.forName(ApplicationConfig.MYSQL_JDBC_DRIVER)
		
		var conn: Connection = null
		var pstmt: PreparedStatement = null
		try{
			// 2. 获取连接
			conn = DriverManager.getConnection(
				ApplicationConfig.MYSQL_JDBC_URL, //
				ApplicationConfig.MYSQL_JDBC_USERNAME, //
				ApplicationConfig.MYSQL_JDBC_PASSWORD //
			)
			val insertSql =
				"""
				  |INSERT INTO
				  |itcast_ads_report.region_stat_analysis(report_date, province, city, count)
				  |VALUES(?, ?, ?, ?)
				  |ON DUPLICATE KEY UPDATE count=VALUES(count)
				  |""".stripMargin
			pstmt = conn.prepareStatement(insertSql)
			
			// 获取当前数据库事务
			val autoCommit: Boolean = conn.getAutoCommit
			conn.setAutoCommit(false)
			
			// 3. 插入数据
			datas.foreach{row =>
				// TODO: 设置参数值
				pstmt.setString(1, row.getAs[String]("report_date"))
				pstmt.setString(2, row.getAs[String]("province"))
				pstmt.setString(3, row.getAs[String]("city"))
				pstmt.setLong(4, row.getAs[Long]("count"))
				
				// 加入批次
				pstmt.addBatch()
			}
			// 批量插入
			pstmt.executeBatch()
			
			// 手动提交
			conn.commit()
			conn.setAutoCommit(autoCommit) // 还原数据库原来事务状态
		}catch {
			case e: Exception => e.printStackTrace()
		}finally {
			// 4. 关闭连接
			if(null != pstmt) pstmt.close()
			if(null != conn) conn.close()
		}
		
	}
	
	/**
	 * 将报表数据保存MySQL数据库，使用SparkSQL自带数据源API接口
	 */
	def saveReportToMySQL(reportDF: DataFrame): Unit = {
		reportDF.write
			// TODO: 采用Append时，如果多次运行报表执行，主键冲突，所以不可用
			//.mode(SaveMode.Append)
			// TODO：采用Overwrite时，将会覆盖以前所有报表，更加不可取
			//.mode(SaveMode.Overwrite)
			.format("jdbc")
			.option("driver", "com.mysql.cj.jdbc.Driver")
			.option("url", "jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
			.option("user", "root")
			.option("password", "123456")
			.option("dbtable", "itcast_ads_report.region_stat_analysis")
			.save()
	}
	
}
