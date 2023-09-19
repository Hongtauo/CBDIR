package DataProcessing

import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_list, monotonically_increasing_id}

// 实现LDA算法
object LDA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("some.spark.config.options", "some.config")
      .appName("test")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    //切换数据库
    spark.catalog.setCurrentDatabase("cbdir")

    // 读取用户-菜品-评分矩阵Matrix
    val df_matrix = spark.read.table("Matrix")


    // 除去第一列，其余的列向量化
    val columnNames = df_matrix.columns.filter(_ != "UserID")
    val assembler = new VectorAssembler()
      .setInputCols(columnNames)
      .setOutputCol("features")

    //output为向量化后的矩阵，以便能输入LDA进行训练
    val output = assembler.transform(df_matrix)
    println("表长为：" + output.count())
    //output.show()

    // 三、使用LDA模型 5130个主题,每个用户表示一个主题
    import org.apache.spark.sql.functions.explode
    import spark.implicits._


    val lda = new LDA()
      .setK(5130)
      .setMaxIter(100) //第一个参数表示主题数，第二个参数是迭代次数

    val model = lda.fit(output)
    // 第一列表示主题，一个用户就是一个主题，第二列表示菜品id，也就是每个主题中的推荐菜品
    val topics = model.describeTopics(5)
    println("The topics described by their top-weighted terms:")

    // 将第二列的推荐菜品id列表展开，去除第三列权重列
    val explodedTopics = topics.select($"topic", explode($"termIndices").as("termIndex"), $"termWeights").drop("termWeights")
    //explodedTopics.show()

    // 四、生成：[用户,推荐]表

    //  1. 对于matrix ，表头为菜品行，提取菜品行得到MealID列表
    val colNames = df_matrix.columns.tail
    val indexedColNames = colNames.zipWithIndex.toMap
    val MealIndexList = indexedColNames.toSeq.toDF("MealID", "Index").orderBy("Index")
    // MealIndexList.show()

    // 将主题模型里面的Index转换为MealID，得到主题模型输出的每个主题对印的菜品推荐TopicsANDMealID
    val topicsWithMealID = explodedTopics.join(MealIndexList, explodedTopics("termIndex") === MealIndexList("Index"))
    val finalTopics = topicsWithMealID.select("topic", "MealID").orderBy("topic")

    // 将同一个topics的推荐菜品ID合并成一行，并命名为MealIDs，MealIDs的值为菜品列表
    val TopicsANDMealID = finalTopics.groupBy("topic").agg(collect_list("MealID").as("MealIDs"))
    TopicsANDMealID.show()

    // 2. 对于matrix 提取用户ID列,建立ID与Index的映射，得到UserIDANDIndex
    // 假设df是你的DataFrame，其中第一列是"UserID"
    val UserIDANDIndex = df_matrix.withColumn("index", monotonically_increasing_id())
    UserIDANDIndex.select("UserID", "index").show()
    //    MealIndexed.show()

    // 3. 生成 用户-菜品推荐 表
    // TopicsANDMealID是你的第一个DataFrame，它包含了"topic"和"MealIDs"两列
    // UserIDANDIndex是你的第二个DataFrame，它包含了"UserID"和"index"两列
    // 使用join操作，根据index和topic进行匹配，最终得到UserIDANDMealID表，这个表就是针对每个用户推荐的菜品ID
    val joinedDF = TopicsANDMealID.join(UserIDANDIndex, TopicsANDMealID("topic") === UserIDANDIndex("index"))
    val UserIDANDMealID = joinedDF.select("UserID", "MealIDs")
    println("针对每个用户推荐的菜品系列")
    UserIDANDMealID.show()

    //将针对每个用户推荐的菜品系列 用户-菜品ID 表存入数据库为UserIDANDMealID表，交由下一个程序处理菜品ID和菜品名的映射
    UserIDANDMealID.write.mode("overwrite").saveAsTable("UserIDANDMealID")
  }
}