
# 本地模式运行Report
SPARK_HOME=/export/server/spark
EXTERNAL_JARS=/root/submit-ads-app/jars
${SPARK_HOME}/bin/spark-submit \
--master local[2] \
--conf spark.sql.shuffle.partitions=2 \
--class cn.itcast.spark.report.PmtReportRunner \
--jars ${EXTERNAL_JARS}/mysql-connector-java-8.0.19.jar,${EXTERNAL_JARS}/protobuf-java-3.6.1.jar,${EXTERNAL_JARS}/config-1.2.1.jar \
hdfs://node1.itcast.cn:8020/spark/apps/spark-ads_2.11-1.0.0.jar

# YARN Cluster集群模式运行
SPARK_HOME=/export/server/spark
EXTERNAL_JARS=/root/submit-ads-app/jars
${SPARK_HOME}/bin/spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 512m \
--executor-memory 512m \
--executor-cores 1 \
--num-executors 2 \
--queue default \
--conf spark.sql.shuffle.partitions=2 \
--class cn.itcast.spark.report.PmtReportRunner \
--jars ${EXTERNAL_JARS}/mysql-connector-java-8.0.19.jar,${EXTERNAL_JARS}/protobuf-java-3.6.1.jar,${EXTERNAL_JARS}/config-1.2.1.jar \
hdfs://node1.itcast.cn:8020/spark/apps/spark-ads_2.11-1.0.0.jar

hdfs dfs -cp /user/root/share/lib/lib_20200422151729/spark/oozie-sharelib-spark-4.1.0-cdh5.16.2.jar /user/root/share/lib/lib_20200422151729/spark2/oozie-sharelib-spark.jar