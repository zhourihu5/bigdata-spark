package cn.itcast.spark.app.mock

/**
 * 用户百度搜索时日志数据封装样例类CaseClass
 * <p>
 *
 * @param sessionId 会话ID
 * @param ip        IP地址
 * @param datetime  搜索日期时间
 * @param keyword   搜索关键词
 */
case class SearchLog(
	                    sessionId: String, //
	                    ip: String, //
	                    datetime: String, //
	                    keyword: String //
                    ) {
	override def toString: String = s"$sessionId,$ip,$datetime,$keyword"
}
