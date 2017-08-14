package com.spark.stream
import com.google.gson.JsonObject

object min_max {
  var list: List[JsonObject] = List()
  var j1 = new JsonObject
  var j2 = new JsonObject
  var j3 = new JsonObject

  j1.addProperty("key1", 1)
  j1.addProperty("key2", 10)
  j2.addProperty("key1", 100)
  j2.addProperty("key2", 120)
  j3.addProperty("key1", 91)
  j3.addProperty("key2", 110)
  j3.addProperty("key1", 11932)
  j3.addProperty("key2", 130)
  list = j1 :: list
  list = j2 :: list
  list = j3 :: list

  def main(args: Array[String]): Unit = {
    var count: Int = 0
    var min: Int = 0
    var max: Int = 0
    var resJson: JsonObject = new JsonObject
    var sum: Double = 0
    list.foreach { x =>
      {
        println(x)
        var iter = x.entrySet().iterator()
        while (iter.hasNext()) {
          var key = iter.next().getKey
          if (!key.equalsIgnoreCase("ts")) {
            var value: Int = x.get(key).getAsInt
            if (count > 0) {
              if (resJson.get(key + "_min").getAsDouble > value) {
                min = value
                resJson.addProperty(key + "_min", min)

              }
              if (resJson.get(key + "_max").getAsDouble < value) {
                max = value
                resJson.addProperty(key + "_max", max)

              }
              sum = resJson.get(key + "_sum").getAsDouble + value

            } else {
              min = value
              max = value
              sum = value
              resJson.addProperty(key + "_min", min)
              resJson.addProperty(key + "_max", max)

            }

            resJson.addProperty(key + "_sum", sum)
          }

        }

      }
      count = count + 1
    }
    println(resJson)
  }
}