package DataProcessing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.collect_list

object MealIDtoName {
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
    // 4. 双表查询
    //菜品名与菜品ID的转化表
    val MealIDtoName = spark.read.table("MealList")
    MealIDtoName.show()

    // 用户-菜品ID推荐表
    val UserIDANDMealID = spark.read.table("UserIDANDMealID")
    UserIDANDMealID.show()

    import spark.implicits._
    val explodedUserIDANDMealID = UserIDANDMealID.select($"UserID", explode($"MealIDs").as("MealIDs"))
    explodedUserIDANDMealID.show()

    val joinedDF = explodedUserIDANDMealID.join(MealIDtoName, explodedUserIDANDMealID("MealIDs") === MealIDtoName("mealID"))
    val UserIDANDMeal = joinedDF.select("UserID", "meal_name")
    val UserIDANDMealList = UserIDANDMeal.groupBy("UserID").agg(collect_list("meal_name").as("meal_name_list"))

    // 用户-菜品 推荐表
    UserIDANDMealList.show()
  }
}
