package LDA_Processing
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession


object Processing {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("some.spark.config.options", "some.config")
      .appName("test")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

// 一、读取清洗后的文件cd_MealData
    //切换数据库
    spark.catalog.setCurrentDatabase("cbdir")

    // 从数据库中读取需要处理的已经清洗过的文件
    val cd_MealData = spark.read.table("cleaned_mealrating")
    cd_MealData.show()

    // 筛选出MealID MealNo 和rating，并计算综合平均分
   val ID_No_Rating = cd_MealData.groupBy("MealID","mealno").agg(avg("Rating").as("AvgMealRating"))
     //.filter("AvgMealRating > 3")
    println("菜品的数量为："+ID_No_Rating.count())
    ID_No_Rating.show()

    // 提取出菜品表，方便下面构建用户-菜品-评分矩阵
    val New_meal_list = ID_No_Rating.dropDuplicates("MealID").select("MealID")
    println("得到菜品ID表，共有："+New_meal_list.count())
    New_meal_list.show()


// 二、构建用户-菜品-评分 分布矩阵，然后使用主题模型LDA进行菜品推荐预测
    // 给DataFrame添加别名
    val df_cd_MealData_alias = cd_MealData.as("cd_MealData")
    val df_New_meal_list_alias = New_meal_list.as("New_meal_list")

    // 使用带有别名的DataFrame进行join操作
    val df_result = df_cd_MealData_alias.join(df_New_meal_list_alias, df_cd_MealData_alias("MealID") === df_New_meal_list_alias("MealID"))

    // 选择需要的列
    val df_final = df_result.select(df_cd_MealData_alias("UserID"), df_cd_MealData_alias("MealID"), df_cd_MealData_alias("Rating"))


    // 使用pivot函数创建二维矩阵，列表示菜品，行表示用户
    val df_pivot = df_final.groupBy("UserID").pivot("MealID").agg(first("Rating"))

    // 将null值替换为0 .repartition(1)表示分区数为1
    val df_matrix = df_pivot.na.fill(0).orderBy("UserID").repartition(1)
    println("构建用户-菜品-评分矩阵：")
    df_matrix.show()

    // 保存matrix到数据库中，LDA训练时直接从存储中读取矩阵，减少内存开销
    df_matrix.write.mode("overwrite").saveAsTable("Matrix")
  }
}
