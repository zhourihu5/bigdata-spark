package cn.itcast.spark.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.{ForeachWriter, Row}

/**
 * 创建类继承ForeachWriter，将数据写入到MySQL表中，泛型为：Row，针对DataFrame操作，每条数据类型就是Row
 */
class MySQLForeachWriter extends ForeachWriter[Row]{
	// 定义变量
	var conn: Connection = _
	var pstmt: PreparedStatement = _
	val insertSQL = "REPLACE INTO `tb_word_count` (`id`, `word`, `count`) VALUES (NULL, ?, ?)"
	
	// Open connection
	override def open(partitionId: Long, epochId: Long): Boolean = {
		// a. 加载驱动类
		Class.forName("com.mysql.cj.jdbc.Driver")
		// b. 获取连接
		conn = DriverManager.getConnection(
			"jdbc:mysql://node1.itcast.cn:3306/db_spark?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true", //
			"root",
			"123456"
		)
		// c. 获取PreparedStatement
		pstmt = conn.prepareStatement(insertSQL)
		//println(s"p-${partitionId}: ${conn}")
		
		// 返回，表示获取连接成功
		true
	}
	
	// Write data to connection
	override def process(row: Row): Unit = {
		// 设置参数
		pstmt.setString(1, row.getAs[String]("value"))
		pstmt.setLong(2, row.getAs[Long]("count"))
		// 执行插入
		pstmt.executeUpdate()
	}
	
	// Close the connection
	override def close(errorOrNull: Throwable): Unit = {
		if(null != pstmt) pstmt.close()
		if(null != conn) conn.close()
	}
}
