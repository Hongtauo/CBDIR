package DataProcessing
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, desc, monotonically_increasing_id, row_number}
import scala.collection.immutable.Nil.sorted
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer

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

    // 将null值替换为0
    val df_matrix = df_pivot.na.fill(0).orderBy("UserID")
    println("构建用户-菜品-评分矩阵：")
    df_matrix.show()

    // 除去第一列，其余的列向量化
    val columnNames = df_matrix.columns.filter(_ != "UserID")
    val assembler = new VectorAssembler()
      .setInputCols(columnNames)
      .setOutputCol("features")

    //output为向量化后的表
    val output = assembler.transform(df_matrix)
    println("表长为："+output.count())
    //output.show()

// 三、使用LDA模型 5130个主题,每个用户表示一个主题
    import org.apache.spark.sql.functions.explode
    import spark.implicits._

//    for (i<-Array(5,10,15,20,40,60,100)){
    val lda = new LDA()
      .setK(5130)
      //.setOptimizer("online")
      .setMaxIter(50) //第一个参数表示主题数，第二个参数是迭代次数

    val model = lda.fit(output)
    // 第一列表示主题，一个用户就是一个主题，第二列表示菜品id，也就是每个主题中的推荐菜品
//    val ll = model.logLikelihood(output)
//    val lp = model.logPerplexity(output)
//    }
    val topics = model.describeTopics(5)
    println("The topics described by their top-weighted terms:")
    //topics.show(false)
    // 将第二列的菜品id展开，
    val explodedTopics = topics.select($"topic", explode($"termIndices").as("termIndex"), $"termWeights").drop("termWeights")
    //explodedTopics.show()

//// 用户菜品推荐
//    // [23.9.18]后面想了一下，其实应该是每个用户ID都是一个主题，所以只需要将matrix中的用户列和菜品列映射到topics即可，也就是有多少个用户，就有多少个topics，下面的代码应该是全都要改掉
//    //
//
//    // New_meal_list转变为索引，这个索引就是topics中的第二列的菜品
//    val indexer = new StringIndexer()
//      .setInputCol("MealID")
//      .setOutputCol("MealIndex")
//      .fit(New_meal_list)
//
//    val indexed = indexer.transform(New_meal_list)
//    println("菜品索引表："+indexed.count())
//    indexed.show()
//
//    // 判断用户属于那个主题，然后将该主题的菜品作为推荐
//    // 计算每个用户对每个菜品的平均打分，找出每个用户评分最高的菜品，根据这个菜品判断是哪个主题
//
//    val user_prefer_meal = cd_MealData.groupBy("UserID","MealID").agg(max("Rating").as("AvgMealRating"))
////    println("用户最喜欢的菜品表："+user_prefer_meal.count())
////    user_prefer_meal.show()
//
//    // 将MealID转换为MealIndex
//    val IDtoINDEX = user_prefer_meal.join(indexed,user_prefer_meal("MealID")===indexed("MealID")).drop("MealID","AvgMealRating")
//    println("IDtoINDEX have："+IDtoINDEX.count())
//    IDtoINDEX.show()


// 四、生成：[用户,推荐]表
    // 对于matrix 提取用户ID列
    import spark.implicits._

    // 1. 对于matrix 提取菜品ID行,建立ID与Index的映射
      // val MealList = df_matrix.columns.tail.toSeq.toDF("MealID")
      //    val MealIndexer = new StringIndexer()
      //        .setInputCol("MealID")
      //        .setOutputCol("MealIndex")
      //        .fit(MealList).transform(MealList)
      //
      //    val MealIndexed = MealIndexer.select("MealID","MealIndex")
    val colNames = df_matrix.columns.tail
    val indexedColNames = colNames.zipWithIndex.toMap
    val MealIndexList = indexedColNames.toSeq.toDF("MealID", "Index").orderBy("Index")
    // MealIndexList.show()

    // 将主题模型里面的Index转换为MealID，得到主题模型输出的每个主题对印的菜品推荐TopicsANDMealID
    val topicsWithMealID = explodedTopics.join(MealIndexList, explodedTopics("termIndex") === MealIndexList("Index"))
    val finalTopics = topicsWithMealID.select("topic", "MealID").orderBy("topic")

    // 假设explodedTopics是你的DataFrame，它包含了"topic"和"termIndex"两列，第二列已经组合成一个列表，列表中就是每个用户推荐的菜品ID
    val TopicsANDMealID = finalTopics.groupBy("topic").agg(collect_list("MealID").as("MealIDs"))
    TopicsANDMealID.show()


    // 2. 对于matrix 提取用户ID行,建立ID与Index的映射，得到UserIDANDIndex
      //    val UserList = df_matrix.select("UserID").orderBy("UserID")
      //    UserList.show()
      //    val UserIndexer = new StringIndexer()
      //          .setInputCol("UserID")
      //          .setOutputCol("UserIndex")
      //          .fit(UserList).transform(UserList)
      //    val UserIndexed = UserIndexer.select("UserID","UserIndex")
      //      UserIndexed.show()

    // 假设df是你的DataFrame，其中第一列是"UserID"
    val UserIDANDIndex = df_matrix.withColumn("index", monotonically_increasing_id())
    UserIDANDIndex.select("UserID","index").show()
    //    MealIndexed.show()

    // 3. 生成 用户-菜品推荐 表
      // TopicsANDMealID是你的第一个DataFrame，它包含了"topic"和"MealIDs"两列
      // UserIDANDIndex是你的第二个DataFrame，它包含了"UserID"和"index"两列
      // 使用join操作，根据index和topic进行匹配
    val joinedDF = TopicsANDMealID.join(UserIDANDIndex, TopicsANDMealID("topic") === UserIDANDIndex("index"))
    val UserIDANDMealID = joinedDF.select("UserID", "MealIDs")
    println("针对每个用户推荐的菜品系列")
    UserIDANDMealID.show()
    UserIDANDMealID.write.mode("overwrite").saveAsTable("UserIDANDMealID")

  }
}
