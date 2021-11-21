package cn.itcast.spark.test.hanlp

import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.tokenizer.StandardTokenizer

/**
 * HanLP 入门案例，基本使用
 */
object HanLPTest {
	
	def main(args: Array[String]): Unit = {
	
		val terms: util.List[Term] = HanLP.segment("汶川地震原因")
		// 导包，将Java集合类对象转换为Scala集合类对象
		import scala.collection.JavaConverters._
		terms.asScala.foreach(term => println(term.word))
		
		// 标准分词
		val terms1: util.List[Term] = StandardTokenizer.segment("放假++端午++重阳")
		terms1.asScala.foreach(term => println(term.word))
	}
	
}
