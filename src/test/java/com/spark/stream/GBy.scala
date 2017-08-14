package com.spark.stream

import org.apache.avro.data.Json

object GBy {
  def main(args: Array[String]): Unit = {
    
  
  val votes = Seq(("scala", 1), ("java", 4), ("scala", 10), ("scala", 1), ("python", 10))
val orderedVotes = votes
  .groupBy(_._1)
  .map { case (which, counts) => 
    (which, counts.foldLeft(0)(_ + _._2))
  }.toSeq
  .sortBy(_._2)
  .reverse
   println(orderedVotes)
}
  //Second Method
   val votes = Seq(("scala", 1), ("java", 4), ("scala", 10), ("scala", 1), ("python", 10))
  val votesByLang = votes groupBy { case (lang, _) => lang }
   println(votesByLang)
val sumByLang = votesByLang map { case (lang, counts) =>
  val countsOnly = counts map { case (_, count) => count }
  (lang, countsOnly.sum)
}
   println(sumByLang)
val orderedVotes = sumByLang.toSeq
  .sortBy { case (_, count) => count }
  .reverse
 println(orderedVotes)
  println("===========================================")
  //

   val myData = Seq(("C001","WS", 1), ("C002","INV",4), ("C001","INV", 10), ("C001","MFM", 1), ("C003","MFM", 10),("C002","MFM", 10))
  
  }



 