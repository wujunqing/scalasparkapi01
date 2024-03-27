package com.myteat



import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class ScalaSparkDataFrameApi {


  def sqlApi(path:String,sql:String):List[Row] = {

   if(path==null||sql==null) {
     println("path:"+path+"sql:"+sql)
     return null
   }

    else {
      val conf = new SparkConf().setMaster("local[*]").setAppName("MySpark")
      // 创建Spark上下文
      val spark: SparkSession = SparkSession.builder()
        .config(conf).getOrCreate()
      val rdd: RDD[Row] = spark.sparkContext.textFile(path)
        .map(_.split(","))
        .map(array => Row(array(0).trim, array(1).trim, array(2).trim, array(3).toInt))

      val coltype = StructType(
        StructField("peer_id", StringType, true) ::
          StructField("id_1", StringType, true) ::
          StructField("id_2", StringType, true) ::
          StructField("year", IntegerType, true) :: Nil
      )
      val df: DataFrame = spark.createDataFrame(rdd, coltype)

      df.createTempView("v_table")



      val frame: DataFrame = spark.sql(sql.stripMargin)

      val list: List[Row]= frame.collect().toList


      spark.stop()
      return list

    }


  }
}
  object  RumApi {
    def main(args: Array[String]): Unit = {
      val api = new ScalaSparkDataFrameApi()
      var sql = String.format(
        """
                    select peer_id
                          ,year
                    from (
                    select peer_id
                          ,year
                          ,v_count
                          ,lead(v_count) over(partition by peer_id order by year desc) v_sum
                    from (
                         select peer_id,year,count(*) v_count
                          from (select peer_id
                                       ,year
                                       ,max(if(instr(peer_id,id_2)>0,year,null)) over(partition by peer_id) v_year
                                 from v_table
                                ) a
                         where v_year>=year
                         group by peer_id,year
                       ) aa
                       ) bb where 1=1 and v_count+v_sum>=%s order by peer_id,year desc
                      """, "3")
      val frame = api.sqlApi("data/data.txt",sql)
      frame.foreach(println)
      //    print("----------")
      //    val frame2 = ScalaSparkDataFrameApi.sqlApi( "data/data2.txt", 5)
      //    frame2.foreach(println)
      //    print("----------")
      //    val frame3 = ScalaSparkDataFrameApi.sqlApi( "data/data2.txt", 7)
      //    frame3.foreach(println)


    }
  }


