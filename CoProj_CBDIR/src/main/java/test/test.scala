package test
import org.apache.spark.sql.SparkSession
import java.util.Properties


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

    //  修改数据库CBDIR的权限的指令“hadoop fs -chmod 777 /user/hive/warehouse/cbdir.db”，将DataFrame文件同步导出到Hive数据库CBDIR下
    mealrating.write.mode("overwrite").saveAsTable("MealRating")

    //  初步分析

    mealrating.groupBy("UserId").count().show()

    mealrating.groupBy("MealID","Rating").count().show()

    mealrating.groupBy("MealID","Review").count().show()

    //  读取外部数据库sql中的文件，需要在IDEA中，从项目结构中导入mysql的驱动包.jar，该驱动包也在工程文件的Configs下
    val pro = new Properties()
    pro.put("driver", "com.mysql.cj.jdbc.Driver")
    pro.put("user", "root")
    pro.put("password", "123456")

    val meal_list = spark.read.jdbc("jdbc:mysql://master:3306","test.meal_list",pro)
    meal_list.show()
    meal_list.write.mode("overwrite").saveAsTable("meal_list")

    // 至此，Hive的数据库CBDIR中存在两张表：1、MealRating 2.meal_list


  }
}
