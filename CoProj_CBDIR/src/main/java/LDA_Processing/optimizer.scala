package LDA_Processing
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.VectorAssembler

  object optimizer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("some.spark.config.options", "some.config")
      .appName("test")
      .master("local[*]")
      .config("spark.sql.debug.maxToStringFields",5200)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // 一、读取清洗后的文件cd_MealData
    //切换数据库
    spark.catalog.setCurrentDatabase("cbdir")

    // 从数据库中读取需要处理的已经清洗过的文件
    val cd_MealData = spark.read.table("cleaned_mealrating")
    //cd_MealData.show()

    // 筛选出MealID MealNo 和rating，并计算综合平均分
    val ID_No_Rating = cd_MealData.groupBy("MealID", "mealno").agg(avg("Rating").as("AvgMealRating"))
    //.filter("AvgMealRating > 3")
    //println("菜品的数量为：" + ID_No_Rating.count())
    //ID_No_Rating.show()

    // 提取出菜品表，方便下面构建用户-菜品-评分矩阵
    val New_meal_list = ID_No_Rating.dropDuplicates("MealID").select("MealID")
    //println("得到菜品ID表，共有：" + New_meal_list.count())
    //New_meal_list.show()


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

    // 将null值替换为0
    val df_matrix = df_pivot.na.fill(0).orderBy("UserID")
    //println("构建用户-菜品-评分矩阵：")
    //df_matrix.show()

    // 除去第一列，其余的列向量化
    val columnNames = df_matrix.columns.filter(_ != "UserID")
    val assembler = new VectorAssembler()
      .setInputCols(columnNames)
      .setOutputCol("features")

    //output为向量化后的表
    val output = assembler.transform(df_matrix).repartition(1)
    //println("表长为：" + output.count())
    //output.show()

    // 三、使用LDA模型 5130个主题,每个用户表示一个主题
    println("参数寻优：")
//    for (i<-Array(5,10,15,20,40,60,100)){
//      println("i="+i)
      val lda = new LDA()
        .setK(5130)
        .setOptimizer("online")
        .setMaxIter(5) //第一个参数表示主题数，第二个参数是迭代次数
      println(lda.getMaxIter)
      val model = lda.fit(output)
      val ll = model.logLikelihood(output)
      val lp = model.logPerplexity(output)

//      println(s"$i $ll")
//      println(s"$i $lp")
    }
//    val topics = model.describeTopics(5)
//    println("The topics described by their top-weighted terms:")
    //topics.show(false)
//  }
}
