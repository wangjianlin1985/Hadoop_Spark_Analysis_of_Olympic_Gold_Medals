package olympicLoad


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties


object CleanCsv {
  def main(args: Array[String]): Unit = {
    println(s"获取到文件输入路径===>${args(0)}")
    val hadoop_cof = new Configuration()
    hadoop_cof.set("fs.defaultFS", args(7))

    val fs: FileSystem = FileSystem.get(hadoop_cof)
    val inPath = new Path(args(0))
    val outPath = new Path(args(1))
    if (fs.exists(inPath)) {
      println("输入路径存在")
    }
    println(s"获取到文件输出路径===>${args(1)}")

    if (fs.exists(outPath)) {
      println("输出路径存在 开始执行删除")
      fs.delete(outPath, true)
    }

    val sparkSql: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("LoadCSV")
      //.enableHiveSupport()
      .getOrCreate()
    import sparkSql.implicits._
    val frame: DataFrame = sparkSql
      .read
      .option("header", "true")
      .option("mdoe", "PERMISSIVE")
      .option("inferSchema", "true")
      .csv("datas/summer.csv")
    frame.createTempView("data")
    //添加mysql 连接
    val properties = new Properties();
    properties.setProperty("user", args(2)); // 用户名
    properties.setProperty("password", args(3)); // 密码
    properties.setProperty("driver", "com.mysql.jdbc.Driver");
    properties.setProperty("numPartitions", "1");
    sparkSql.sql("select * from data")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(s"jdbc:mysql://${args(5)}:${args(6)}/${args(4)}", args(9), properties)
      println(s"jdbc:mysql://${args(5)}:${args(6)}/${args(4)}")

    //过滤出主要字段
    sparkSql.sql("select Year,Country ,Medal from data").createTempView("data1")
    //统计每年每个国家每种奖牌获取次数
    sparkSql.sql(" select Year,Country ,Medal,count(*) as cu from data1 group by Year,Country ,Medal")
      .createTempView("data2")
    //对统计的结果进行倒置  对空值进行填充
    val result: DataFrame = sparkSql.sql(" select * from data2 ")
      .groupBy("Year", "Country")
      .pivot("Medal", Seq("Gold", "Silver", "Bronze"))
      .sum("cu")
      .na
      .fill(0)

    //重置分区对方便hdfs的保存 并保存到hdfs
    result.repartition(1)
      .write.option("encoding", "utf-8")
      .csv(args(1))

    //写入mysql
    result
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(s"jdbc:mysql://${args(5)}:${args(6)}/${args(4)}",args(8), properties)
  }
}



//case class complete(year:String,country:String,num:String)

//RDD 实现代码
//select keyId,isnull(info,0) as info from test
//pivot ( sum('cu') as 12  for 'Medal' in( 'Gold','Silver','Bronze'))
//pivot (sum('count(1)' for 'Medal' in ('Gold','Silver','Bronze')))

/* Clean.rdd
   .map((x: Row) => ((x(0), x(1), x(2)), 1))
   .repartition(1)
   .reduceByKey(_+_)
   .map((x: ((Any, Any, Any), Int)) => ((x._1._1, x._1._2), (x._1._3, x._2)))
   .groupByKey()
   .mapValues(_.toList.toBuffer)
   .mapValues(x => {
     val lists = List("Gold", "Bronze", "Silver")

     val temporary = new ListBuffer[String]

     val buffer: mutable.Buffer[String] = x.map(_._1.toString)

     if (x.size <= 2) for (elem <- lists) if (!buffer.contains(elem)) temporary.append(elem)

     if (temporary.nonEmpty) for (elem <- temporary) x.append((elem, 0))
     x.sortBy(_._1.toString).toList
   })
   .mapValues(x=>{
     val list: List[Int] = x.map(_._2)
     (list(1),list(2),list.head)

     })
*/


// .toDF()
// .write.mode("overwrite").option("encoding","utf-8")
// .csv("hdfs://172.17.3.173:8020/demoData/olympic2")

// .saveAsTextFile("hdfs://172.17.3.173:8020/demoData/olympic1")


/*
   drop table if exists default.olympic ;
    create table default.olympic(
    Year string,
    Country string,
    Gold int,
    Silver int,
    Bronze int
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

sqoop import --connect jdbc:mysql://172.17.3.174:3306/book  --username root --password %Root123456 --table olympic --fields-terminated-by '\t' --delete-target-dir --num-mappers 1 --hive-import --hive-database default --hive-table olympic --hive-overwrite





* */