
# 本地模式运行
SPARK_HOME=/export/server/spark
EXTERNAL_JARS=/root/submit-ads-app/jars
${SPARK_HOME}/bin/spark-submit \
--master local[2] \
--conf "spark.sql.shuffle.partitions=2" \
--class cn.itcast.spark.etl.PmtEtlRunner \
--jars ${EXTERNAL_JARS}/ip2region-1.7.2.jar,${EXTERNAL_JARS}/config-1.2.1.jar \
hdfs://node1.itcast.cn:8020/spark/apps/spark-ads_2.11-1.0.0.jar

# YARN Client 集群模式运行
SPARK_HOME=/export/server/spark
EXTERNAL_JARS=/root/submit-ads-app/jars
${SPARK_HOME}/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 512m \
--executor-memory 512m \
--executor-cores 1 \
--num-executors 2 \
--queue default \
--conf spark.sql.shuffle.partitions=2 \
--class cn.itcast.spark.etl.PmtEtlRunner \
--jars ${EXTERNAL_JARS}/ip2region-1.7.2.jar,${EXTERNAL_JARS}/config-1.2.1.jar \
hdfs://node1.itcast.cn:8020/spark/apps/spark-ads_2.11-1.0.0.jar


