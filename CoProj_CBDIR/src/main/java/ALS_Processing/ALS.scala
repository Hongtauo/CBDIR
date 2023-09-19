package ALS_Processing

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{collect_list, monotonically_increasing_id}

object ALS {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .appName("ALSModel")
      .master("local[3]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    //    读取数据
    spark.catalog.setCurrentDatabase("cbdir")
    val MealRating = spark.read.table("cleaned_mealrating")
    MealRating.show()
    //MealRating.repartition(1).write.mode("overwrite").csv("D:\\MyFiles\\Practice\\Project\\Proj2_CBDIR\\Data\\cleaned_mealrating.csv")

    // 得到菜品ID到Index的映射表
    val MealIDTOIndex = MealRating.select("MealID","MealIndex").distinct().orderBy("MealIndex")
    MealIDTOIndex.show()

    // 得到用户ID到Index的映射表
    val UserIDTOIndex = MealRating.select("UserID", "UserIndex").distinct().orderBy("UserIndex")
    UserIDTOIndex.show()

    // 得到MealID到MealName的映射表
    val MealIDTOName = MealRating.select("MealID", "meal_name").distinct().orderBy("MealID")
    MealIDTOName.show()

    val TrainTable = MealRating.drop("MealID","UserID","Review","ReviewTime","mealno","meal_name").orderBy("UserIndex")
    println("去除无关列：")
    TrainTable.show()

    // 切分数据集

    // 使用 sample 方法按比例抽取数据
    val train = TrainTable.sample(false, 0.8)
    val test = TrainTable.sample(false, 0.2)

    println("训练数据集，数据量为："+train.count())
    train.show()

    println("测试数据集，数据量为：" + test.count())
    test.show()

    // ALS(协同过滤)需要“UserIndex，MealIndex，Rating”，三个数据字段必须数值类型
    val als = new ALS()
      //必选参数
      .setItemCol("MealIndex")
      .setUserCol("UserIndex")
      .setRatingCol("Rating")
      //可选参数
      .setRank(10)
      .setAlpha(10)
      .setMaxIter(10)
      .setImplicitPrefs(true)
      .setRegParam(0.1)

    val model = als.fit(train)
    model.setColdStartStrategy("drop")
    val pre = model.transform(test)
    pre.show(10, false)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("Rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(pre)
    println("均方误差=" + rmse)

    model.userFactors.orderBy("id")
    model.itemFactors.orderBy("id")

    val userRecs = model.recommendForAllUsers(10)
    userRecs.show()

    // 还原UserName
    val df_final = userRecs.join(UserIDTOIndex, userRecs("UserIndex") === UserIDTOIndex("UserIndex"))
      .drop("UserIndex")
      .withColumnRenamed("UserName", "UserIndex")

    df_final.show()


    import org.apache.spark.sql.functions.explode

    val df =  df_final
    import spark.implicits._

    // 使用explode函数展开'recommendations'列
    val df_exploded = df.select($"UserID", explode($"recommendations").as("recommendation"))
    // 提取MealIndex，并删除rating
    val df_new = df_exploded.select($"UserID", $"recommendation.MealIndex".as("MealIndex"))
    df_new.show()

    // 使用join操作,将Meal的Index映射ID
    val UserANDMealID = df_new.join(MealIDTOIndex, df_new("MealIndex") === MealIDTOIndex("MealIndex"))
      .drop("MealIndex")
      .withColumnRenamed("MealIndex", "MealID")

    // 使用join操作,将Meal的ID映射为Name
    val UserANDMealName = UserANDMealID.join(MealIDTOName, UserANDMealID("MealID") === MealIDTOName("MealID"))
      .drop("MealID")
      .withColumnRenamed("meal_name", "MealName")
    UserANDMealName.show()

    val UserIDANDMeal = UserANDMealName.select("UserID", "MealName")
    val UserIDANDMealList = UserIDANDMeal.groupBy("UserID").agg(collect_list("MealName").as("meal_name_list"))

    UserIDANDMealList.show()
    UserIDANDMealList.write.mode("overwrite").saveAsTable("ALSRecommendList")
  }
}
