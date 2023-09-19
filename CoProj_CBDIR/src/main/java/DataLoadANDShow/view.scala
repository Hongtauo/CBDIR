package DataLoadANDShow
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession

object view {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("some.spark.config.options","some.config")
      .appName("test")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // 从本计算机读取数据
    val empdata = spark.read.json("D:\\MyFiles\\Practice\\Project\\Proj2_CBDIR\\Data\\MealRatings_201705_201706.json")
    val meal_list = spark.read.json("./Data/meal_list.json")

    // 查看数据
    empdata.show()  //查看发现数据时间范围为5月1到6月30
    //统计每个用户的评价数目
    empdata.groupBy("UserId").count().show()
    //统计每个菜品各评分的数目
    empdata.groupBy("MealID","Rating").count().show()
    //统计每个菜品相同评价内容的数目
    empdata.groupBy("MealID","Review").count().show()
    //每种评分的占比
    val RatingType = empdata.groupBy("Rating")
      .agg(
        count("Rating").alias("sum")
      )
      .withColumn("weights",round(col("sum")/empdata.count()*100,3))
    RatingType.show()
    // 保存为csv文件
    RatingType.write.csv("./CoProj_CBDIR/Data/RatingType.csv")
//    RatingType.write.json("./Data/RatingType.json")


    //按评分对数据进行分组再按菜品ID分组
    val result1 = empdata.groupBy("Rating","MealID")
      .agg(
        //统计每个评分等级中每种菜品的记录数量并记录为新列Count
        count("MealID").alias("Count")
      )

    val result2 = result1.groupBy("Rating")
      .agg(
        //统计每种评分等级里出现次数最多的菜品
        max("Count").alias("MaxCount"),
        first("MealID").alias("MealID")
      )
      .join(meal_list,Seq("MealID"))

    // 保存为csv文件
    result2.write.csv("./CoProj_CBDIR/Data/result2.csv")
//    result2.write.json("./Data/result2.json")
    result2.show()

    val result3 = result1.groupBy("Rating")
      .agg(
        //统计每种评分等级里出现次数最少的菜品
        min("Count").alias("MinCount"),
        first("MealID").alias("MealID")
      )
      .join(meal_list,Seq("MealID"))

    //保存为csv文件
    result3.write.csv("./Data/result3.csv")
//    result3.write.json("./Data/result3.json")
    result3.show()
    // 检查是否存在相同用户对同一菜品的重复评分记录，并输出重复记录总数
    // 按用户ID对数据进行分组再按菜品ID分组
    val duplicateRecords = empdata.groupBy("UserID", "MealID")
      //统计其中非空行的数量并记录为新列Count
      .agg(count("*").alias("Count"))
      //如果Count>1则存在相同用户对同一菜品重复评分，筛选Count>1的数据
      .filter(col("Count") > 1)
    duplicateRecords.show()
    // 验证发现每个重复评论的用户只会对同一菜品进行2次评分
    //duplicateRecords.show(numRows = 100)
    //val t = duplicateRecords.where(col("Count")>2)
    //t.show() //t为空
    //输出重复记录数据的数量
    print("重复记录数据的数量"+duplicateRecords.select(sum("Count")).first()(0))
    print("\n")
    //对重复数据集进行明细查询。（为什么会出现相同用户重复评分，引起这个现象的原因是？）
    val windowSpec2 = Window.partitionBy(col("UserID"), col("MealID"))
      .orderBy(col("ReviewTime"))
    val duplicateRows = empdata.withColumn("Count", row_number()
      .over(windowSpec2))
    val filteredRows = duplicateRows.where(col("Count") > 1)
      .select(col("MealID"), col("Rating"), col("Review"), col("ReviewTime"), col("UserID"))
    //将Review转换成年月日格式
    val formattedDf = filteredRows.withColumn("日期", date_format(from_unixtime(col("ReviewTime")), "yyyy-MM-dd"))
    //展示重复数据
    formattedDf.show()
    //formattedDf.show(2518)
    //查询具体用户对哪些菜品进行了重复评分
    val filteredDf = formattedDf.filter(col("UserID") === "A10D86IGYSAP0N")
    filteredDf.show()
    //查询具体用户对具体菜品的重复评分情况
    val filteredDf2 = empdata.filter(col("UserID") === "A10D86IGYSAP0N" && col("MealID") === "B00I3MNGCG")
      .withColumn("日期",date_format(from_unixtime(col("ReviewTime")), "yyyy-MM-dd"))
    filteredDf2.show()


    //统计销售量前10的菜品
    val top10 = empdata.groupBy("MealID")
      .agg(
        count("*").alias("Sales")
      )
      .orderBy(desc("Sales"))
      .limit(10)
      .join(meal_list,Seq("MealID"))
    top10.show()

    top10.write.csv("./CoProj_CBDIR/Data/top10.csv")
//    top10.write.json("./Data/top10.json")



    //统计好评前10的菜品
    val RatingTop10 = result1.where(col("Rating")==="5.0")
      .orderBy(desc("Count"))
      .limit(10)
      .join(meal_list,Seq("MealID"))
    RatingTop10.show()

    RatingTop10.write.csv("./CoProj_CBDIR/Data/RatingTop10.csv")
//    RatingTop10.write.json("./Data/RatingTop10.json")

  }

}
