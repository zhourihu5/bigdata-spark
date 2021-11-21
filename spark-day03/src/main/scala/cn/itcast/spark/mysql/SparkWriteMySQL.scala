package cn.itcast.spark.mysql

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将RDD数据保存至MySQL表中
 */
object SparkWriteMySQL {
	
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
		val inputRDD: RDD[String] = sc.textFile("datas/wordcount.data", minPartitions = 2)
		
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
		
		// TODO: 保存结果数据RDD至MySQ表中
		resultRDD
			// 降低分区数
			.coalesce(1)
			// 针对分区操作
            .foreachPartition{iter => saveToMySQL(iter)
			    //val xx: Iterator[(String, Int)] = iter
		    }
		// 应用结束，关闭资源
		sc.stop()
	}
	
	
	/**
	 * 将RDD每个分区数据保存至MySQL表汇总
	 * @param datas 分区数据，封装在Iterator中
	 */
	def saveToMySQL(datas: Iterator[(String, Int)]): Unit = {
		
		// 1. 加载驱动类
		Class.forName("com.mysql.cj.jdbc.Driver")
		
		var conn: Connection = null
		var pstmt: PreparedStatement = null
		
		try{
			// 2. 获取连接
			conn = DriverManager.getConnection(
				"jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
					"root",
				"123456"
			)
			val insertSql = "INSERT INTO db_test.tb_wordcount (word, count) VALUES(?, ?)"
			pstmt = conn.prepareStatement(insertSql)
			
			// 获取当前数据库事务
			val autoCommit: Boolean = conn.getAutoCommit
			conn.setAutoCommit(false)
			
			// 3. 插入数据
			datas.foreach{case(word, count) =>
				// TODO: 设置参数值
				pstmt.setString(1, word)
				pstmt.setString(2, count.toString)
				
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
	
}
