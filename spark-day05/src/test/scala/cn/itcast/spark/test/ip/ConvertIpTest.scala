package cn.itcast.spark.test.ip

import cn.itcast.spark.config.ApplicationConfig
import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}

/**
 * 测试使用ip2Region工具库解析IP地址为省份和城市
 */
object ConvertIpTest {
	
	def main(args: Array[String]): Unit = {
		
		// 1. 创建DbSearch对象，传递字典数据
		val dbSearcher = new DbSearcher(new DbConfig(), ApplicationConfig.IPS_DATA_REGION_PATH)
		
		// 2. 传递IP地址，进行解析
		val dataBlock: DataBlock = dbSearcher.btreeSearch("36.58.93.58")
		val region: String = dataBlock.getRegion
		// 中国|0|北京|北京市|阿里云
		println(region)
		
		// 3. 提取省份和城市
		val Array(_, _, province, city, _)  = region.split("\\|")
		println(s"省份 = $province, 城市 = $city")
		
	}
	
}
