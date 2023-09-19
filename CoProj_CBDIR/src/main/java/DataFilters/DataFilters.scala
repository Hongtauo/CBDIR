package DataFilters
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, desc, monotonically_increasing_id, row_number}
import java.util.Properties


object DataFilters {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder()
      .config("some.spark.config.options","some.config")
      .appName("test")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // 读取HDFS上的结构化数据
    val mealrating = spark.read.json("hdfs://master:8020/CBDIRDatas/MealRatings_201705_201706.json")

//    mealrating.show()

    //  切换使用的数据库
    spark.catalog.setCurrentDatabase("cbdir");

    //  修改数据库CBDIR的权限的指令“hadoop fs -chmod 777 /user/hive/warehouse/cbdir.db”，将DataFrame文件同步导出到Hive数据库CBDIR下

//    mealrating.write.mode("overwrite").saveAsTable("MealRating")


    //  读取外部数据库sql中的文件，需要在IDEA中，从项目结构中导入mysql的驱动包.jar，该驱动包也在工程文件的Configs下
    val pro = new Properties()
    pro.put("driver", "com.mysql.cj.jdbc.Driver")
    pro.put("user", "root")
    pro.put("password", "123456")

    val meal_list = spark.read.jdbc("jdbc:mysql://master:3306","test.meal_list",pro)
//    meal_list.show()
//    meal_list.write.mode("overwrite").saveAsTable("meal_list")

    // 至此，Hive的数据库CBDIR中存在两张表：1、MealRating 2.meal_list

    //过滤重复评分数据
    // 如果报Could not locate executable null\bin\winutils.exe in the Hadoop binaries的错误，在Config文件夹下找到winutils-master，并按此网站的教程设置环境变量https://gitcode.net/mirrors/cdarlint/winutils，版本选择大版本相同的即可
    // 定义窗口规范，按UserID和MealID分区并按ReviewTime降序排序
    val windowSpec = Window.partitionBy("UserID", "MealID").orderBy(desc("ReviewTime"))

    // 使用窗口函数为每个分区中的记录分配排名
    val rankedDF = mealrating.withColumn("rank", row_number().over(windowSpec))

    // 保留排名为1的记录，即每个用户对菜品的最新评分记录
    val latestRatings = rankedDF.filter(col("rank") === 1).drop("rank")

    latestRatings.show(false)
    // 输出过滤掉重复数据后的数据数量
    print("过滤掉重复数据后的数据数量:"+latestRatings.select("UserID").count())
    print("\n")


    //数据变换，将数据转换成“适当的”格式，以适应挖掘任务及算法的需要
    // 为用户和菜品创建索引
    val userIndex = latestRatings.select("UserID").distinct().withColumn("UserIndex", monotonically_increasing_id())
    val mealIndex = latestRatings.select("MealID").distinct().withColumn("MealIndex", monotonically_increasing_id())

    // 使用索引值替换原始数据中的用户和菜品
    val encodedData = latestRatings
      .join(userIndex, Seq("UserID"))
      .join(mealIndex, Seq("MealID"))
      .join(meal_list,Seq("MealID"))
//      .drop("UserID", "MealID")
//      .withColumnRenamed("UserIndex", "UserID")
//      .withColumnRenamed("MealIndex", "MealID")
    //展示编码后的数据
    println("编码后的数据")
    encodedData.show()

    // 将DataFrame数据保存至Hive,在数据库cbdir中成为一张新的表‘cleaned_mealrating’
    encodedData.repartition(1).write.mode("overwrite"). saveAsTable("cbdir.cleaned_mealrating")

    // 计算要分配的行数
    val totalRows = encodedData.count()
    val firstPartitionSize = (totalRows * 0.8).toLong
    val secondPartitionSize = (totalRows * 0.1).toLong

    // 使用 sample 方法按比例抽取数据
    val firstPartitionData = encodedData.sample(false, 0.8)
    val secondPartitionData = encodedData.sample(false, 0.1)
    val thirdPartitionData = encodedData.sample(false, 0.1)


    // 保存分区数据为JSON文件
    firstPartitionData.write.json("Data/traindata.json")
    secondPartitionData.write.json("Data/testdata.json")
    thirdPartitionData.write.json("Data/verifydata.json")

  }
}