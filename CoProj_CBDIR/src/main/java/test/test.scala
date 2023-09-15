package test
import org.apache.spark.sql.SparkSession

object test {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // 读取HDFS上的结构化数据
    val mealrating = spark.read.json("hdfs://master:8020/CBDIRDatas/MealRatings_201705_201706.json")
    mealrating.show()

    //  切换使用的数据库
    spark.catalog.setCurrentDatabase("cbdir")

    //  修改数据库CBDIR的权限的指令“hadoop fs -chmod 777 /user/hive/warehouse/cbdir.db”
    mealrating.write.mode("overwrite").saveAsTable("MealRating")

    //  初步分析

    mealrating.groupBy("UserId").count().show()

    mealrating.groupBy("MealID","Rating").count().show()

    mealrating.groupBy("MealID","Review").count().show()



  }
}
