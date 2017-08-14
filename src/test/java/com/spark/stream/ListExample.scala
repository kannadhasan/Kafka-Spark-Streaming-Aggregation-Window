package com.spark.stream

object ListExampl {
  def main(args: Array[String]): Unit = {
    
     val unsorted = Map("32" -> List(22,11,34), "01" -> List(34,12,14,23), "30" -> List(34,12,14,23))
    println(unsorted.toSeq.sortBy(_._1))
     
     
     
     
println("Demo")
//tuble
val a=(20,4,8)
println(a._1)
println(a._3)
//List
val l=List(2,3,5,6,78,9,3)
for(x<-l){
  println(x)
  
}
//reduce
val sum=l.reduce((x:Int,y:Int)=>x+y)
println(sum)

val fil=l.filter { x => x!=3 }
println(fil)

println(l.reverse)
println(l.reverse.sorted)
println(l.sorted)
println(l.max)
println(l.min)
println(l.distinct)
println(l.sum)

//Map
var MyMap=Map("a"->"Kanna","b"->"thasan")
println(MyMap("a"))

val acd=util.Try(MyMap("c")) getOrElse "UnKnown"

println(acd)



    
  }
 
}