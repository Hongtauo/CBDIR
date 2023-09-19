package LDA_Processing

import org.apache.spark.ml.clustering.LDA

object test {
  def main(args: Array[String]): Unit = {

    for (i <- Array(5, 10, 15, 20, 40, 60, 100)) {
      println("i=" + i)
      val lda = new LDA()
        .setK(5130)
        .setOptimizer("online")
        .setMaxIter(i) //第一个参数表示主题数，第二个参数是迭代次数
      println(lda.getMaxIter)
      //      val model = lda.fit(output)
      //      val ll = model.logLikelihood(output)
      //      val lp = model.logPerplexity(output)

      //      println(s"$i $ll")
      //      println(s"$i $lp")
    }
  }
}
