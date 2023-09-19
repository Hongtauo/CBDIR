# CBDIR
# 一、项目介绍
# 二、数据说明

1. Hadoop集群数据说明
   
   在Hadoop中，将meal_list.sql与MealRatings_201705_201706.json两个文件从虚拟机本地存储/opt目录上传到hdfs中
   1. 对于meal_list.sql文件，进入mysql，使用批处理命令将其写入mysql本地数据库中
   2. 对于MealRatings_201705_201706.json，此文件保存到hdfs中即可
2. Hive说明
   
   在Hive中，创建CBDIR数据库，用于保存原始数据表和处理后的数据表
3. spark说明
   
   1. MealRatings_201705_201706.json文件在spark可以以格式化方式读入，并写入hive的CBDIR数据库中，命名为“MealRating”
   2. meal_list.sql文件可以在spark中以外部数据库方式读入，并写入hive的CBDIR数据库中，命名为“meal_list”
4. 可能的报错说明
   
   1. 读取外部数据库sql中的文件，需要在IDEA中，从项目结构中导入mysql的驱动包.jar，该驱动包也在工程文件的Configs下
   2. 报Could not locate executable null\bin\winutils.exe in the Hadoop binaries的错误，在Config文件夹下找到winutils-master，并按此网站的教程设置环境变量https://gitcode.net/mirrors/cdarlint/winutils，版本选择大版本相同的即可

# 三、工程介绍
1. 项目文件结构

   工程文件：CoProj_CBDIR
2. 算法说明

   1. LDA 主题模型算法

      1. **数据预处理**：首先，我们需要将用户-菜品-评分数据构造成一个矩阵，其中每一行代表一个用户，每一列代表一种菜品，矩阵中的每个元素代表对应用户对对应菜品的评分。这个矩阵可以看作是文档-词频矩阵，其中每个用户是一个“文档”，每种菜品是一个“词”，评分是“词频”。

      2. **模型训练**：然后，我们将这个矩阵输入到LDA模型中进行训练。在这个过程中，LDA模型会学习到每个用户（即“文档”）的主题分布，以及每个主题的菜品（即“词”）分布。

      3. **主题提取**：训练完成后，我们可以从LDA模型中提取出每个用户的主题分布。这个主题分布可以看作是用户的兴趣模型，其中每个主题代表一种兴趣，主题的权重代表了用户对这种兴趣的偏好程度。

      4. **推荐生成**：最后，我们可以根据用户的主题分布来生成推荐。具体来说，我们可以为每个主题选择权重最高的topk个菜品作为推荐结果。这样，我们就得到了一个基于用户兴趣的推荐列表。


   2. LSA 协同过滤算法
3. 过程

4. 结果

# 四、引用