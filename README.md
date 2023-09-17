# CBDIR
# 说明

1. Hadoop集群数据说明
   
   在Hadoop中，将meal_list.sql与MealRatings_201705_201706.json两个文件从虚拟机本地存储/opt目录上传到hdfs中
   1. 对于meal_list.sql文件，进入mysql，使用批处理命令将其写入mysql本地数据库中
   2. 对于MealRatings_201705_201706.json，此文件保存到hdfs中即可
3. Hive说明
   
   在Hive中，创建CBDIR数据库，用于保存原始数据表和处理后的数据表
5. spark说明
   
   1. MealRatings_201705_201706.json文件在spark可以以格式化方式读入，并写入hive的CBDIR数据库中，命名为“MealRating”
   2. meal_list.sql文件可以在spark中以外部数据库方式读入，并写入hive的CBDIR数据库中，命名为“meal_list”
7. 可能的报错说明
   
   1. 读取外部数据库sql中的文件，需要在IDEA中，从项目结构中导入mysql的驱动包.jar，该驱动包也在工程文件的Configs下
   2. 报Could not locate executable null\bin\winutils.exe in the Hadoop binaries的错误，在Config文件夹下找到winutils-master，并按此网站的教程设置环境变量https://gitcode.net/mirrors/cdarlint/winutils，版本选择大版本相同的即可
