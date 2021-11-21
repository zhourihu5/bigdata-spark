package cn.itcast.spark.convert

/**
 * 封装电影评分数据
 *
 * @param userId    用户ID
 * @param itemId    电影ID
 * @param rating    用户对电影评分
 * @param timestamp 评分时间戳
 */
case class MovieRating(
	                      userId: String,
	                      itemId: String,
	                      rating: Double,
	                      timestamp: Long
                      )