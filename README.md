# G-Spark

G-Spark是在标准Apache Spark 2.1.4分支4d2d3d47提交的基础上修改实现的一个集群分布式计算框架, 
目前能够支持原生的多集群部署, 支持处理分布在多个集群的数据. 

目前框架只支持基于RDD的运算, 尚不支持标准Spark的其他模块.

## 构建G-Spark

参见[标准Spark构建](http://spark.apache.org/docs/latest/building-spark.html).

推荐使用`build/sbt -Phadoop=2.9`, 在处理多HDFS集群数据时, 依赖于通过`http REST`的方式
与各个HDFS集群的Namenode交互. 而Hadoop在2.9.x版本之前, 其WebHdfsFileSystem的API实现有缺陷.

## 示例

原则上, 除了在HDFS数据读取和写入上与标准Spark不同外, 其余方面G-Spark与标准Spark在程序API上是相同的.

以简单的Wordcount为例:

```scala
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.spark.{SparkConf, SparkContext}

object MCWordCount {
  def main(args: Array[String]): Unit = {
    val statPath = "/storage/bd_workdir/spark/statDir"
    val conf = new SparkConf().setAppName("MCWordCount")
    val sc = new SparkContext(conf)

    // 指定数据在每个HDFS集群上的数据路径
    val paths = Map(
      "act-32-36" -> s"/user/lizp/HiBench/Wordcount/Input/",
      "act-37-41" -> s"/user/lizp/HiBench/Wordcount/Input/",
      "act-42-46" -> s"/user/lizp/HiBench/Wordcount/Input/"
    )
    // 新式的多HDFS集群数据的读入API
    val rdd1 = sc.newMCHadoopFile(
      paths,
      classOf[SequenceFileInputFormat[NullWritable, Text]],
      classOf[NullWritable],
      classOf[Text]
    )
    val words = rdd1.map(_._2.toString).flatMap(_.split("\\s+"))
    val count = words.map((_, 1)).reduceByKey(_+_)
    count.collect().foreach(println)

    sc.stop()
  }
}
```

## 配置

最重要的一个配置是各个集群的带宽, 虽然带宽可以选择实时测量或直接设定固定的值, 考虑到带宽测量本身的悖论, 推荐直接
设定带宽值:

```conf
spark.siteMaster.names                  act-32-36,act-37-41,act-42-46
spark.act-32-36.bandwidth               40mb
spark.act-37-41.bandwidth               20mb
spark.act-42-46.bandwidth               60mb
```

另一个重要的配置是统计, 你可以选择将应用运行过程中的各个测量量记录到文件中, 比如在SubStage的每个Task花费的时间,
每个SiteDriver远程Shuffle拉取的数据量等.

```conf
// 每个集群的统计, 主要包含具体的各个任务的统计量和SubStage相关的统计量
spark.siteAppStat.enabled               true
spark.siteAppStat.path                  /storage/bd_workdir/spark/statDir
// 整个应用的统计, 主要包含Stage和App相关的统计量
spark.appStat.enabled                   true
spark.appStat.path                      /storage/bd_workdir/spark/statDir
```