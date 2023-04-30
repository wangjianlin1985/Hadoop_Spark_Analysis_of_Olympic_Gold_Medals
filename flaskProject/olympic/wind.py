import os
from olympic import confInfo


# 格式化hdfs路径方法
def getHdfsPath(PATH):
    return "{}{}".format(confInfo.HADOOP_PORT, PATH)


# 文件 上传hdfs
print("{}bin/hdfs dfs -put {} {}".format(confInfo.HADOOP_HOME, confInfo.INPUT_CSV, confInfo.HDFS_FILE))


# 提交spark任务
print("{}bin/spark-submit --class olympicLoad.CleanCsv --master local[*] {} {} {} {} {} {} {} {} {} {} {}"
      .format(confInfo.SPARK_HOME,
              confInfo.INPUT_JARS,
              getHdfsPath(confInfo.HDFS_FILE),
              getHdfsPath(confInfo.SPARK_OUT_FILE),
              confInfo.MYSQL_USER,
              confInfo.MYSQL_PASSWORD,
              confInfo.MYSQL_DATABASE,
              confInfo.IP,
              confInfo.MYSQL_PORT,
              confInfo.HADOOP_PORT,
              confInfo.TABLE,
              confInfo.MYSQL_QU_TABLE
              ))


# sqoop数据抽取到hive
print("{}bin/SQOOP import --connect jdbc:mysql://{}:{}/{}  --username {} --password {} "
      "--table olympic --fields-terminated-by '\t' --delete-target-dir --num-mappers 1 "
      "--hive-import --hive-database default --hive-table olympic --hive-overwrite".format(confInfo.SQOOP_HOME,
                                                                                           confInfo.IP,
                                                                                           confInfo.MYSQL_PORT,
                                                                                           confInfo.MYSQL_DATABASE,
                                                                                           confInfo.MYSQL_USER,
                                                                                           confInfo.MYSQL_PASSWORD,
                                                                                           confInfo.TABLE,
                                                                                           confInfo.TABLE
                                                                                           ))



#sqoop如果导入报错：java.lang.IncompatibleClassChangeError: Found class jline.Terminal, but interface was expecte
#将hive下的新版本jline的JAR包拷贝到hadoop下：jline-2.12.jar => D:\Software\bigdata\hadoop-2.7.7\share\hadoop\yarn\lib


'''
drop table if exists default.olympic ;
create table default.olympic(
        Year string,
        Country string,
        Gold int,
        Silver int,
        Bronze int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

'''
