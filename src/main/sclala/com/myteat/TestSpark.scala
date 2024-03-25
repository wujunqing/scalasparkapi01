package com.myteat

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object TestSpark {

  //    1. For each peer_id, get the year when peer_id contains id_2, for example for ‘ABC17969(AB)’ year is 2022.
  //    使用 INSTR 函数和 when 表达式来检查 "peer_id" 字段中是否包含 "id_2"
  //    如果包含，则标记为 "Yes"，否则标记为 "No"
  def step1(spark: SparkSession):DataFrame = {

    val step1: DataFrame=spark.sql(
      """
        |select peer_id
        |      ,id_1
        |      ,id_2
        |      ,year
        |      ,if(instr(peer_id,id_2)>0,year,null) v_year
        |   from v_table
        |""".stripMargin)

    return step1

  }



  //  2.	Given a size number, for example 3. For each peer_id count the number of each year (which is smaller or equal than the year in step1).
  //    For example, for ‘ABC17969(AB)’, the count should be:
  // Given a size number, for example 3


  def step2(spark: SparkSession):DataFrame = {



    val step2:DataFrame= spark.sql(
      """
        |select year,count(*) v_count
        |from (select peer_id
        |      ,year
        |      ,max(if(instr(peer_id,id_2)>0,year,null)) over(partition by peer_id) v_year
        |   from v_table
        |   ) a
        |     where v_year>=year
        |     group by peer_id,year
        |""".stripMargin)

   return  step2

  }

  //    3.	Order the value in step 2 by year and check if the count number of the first year is bigger or equal than the given size number. If yes, just return the year.
  //    If not, plus the count number from the biggest year to next year until the count number is bigger or equal than the given number. For example, for ‘AE686(AE)’, the year is 2023, and count are:
  def step3(spark: SparkSession):DataFrame = {

    //    3.	Order the value in step 2 by year and check if the count number of the first year is bigger or equal than the given size number. If yes, just return the year.
    //    If not, plus the count number from the biggest year to next year until the count number is bigger or equal than the given number. For example, for ‘AE686(AE)’, the year is 2023, and count are:

    var sql = String.format("""
                    select peer_id
                          ,year
                          ,v_count
                          ,v_count+v_sum
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
                      ""","3")
    val step3:DataFrame= spark.sql(sql.stripMargin);


    return  step3

  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("MySpark")
    // 创建Spark上下文
    val spark: SparkSession = SparkSession.builder()
      .config(conf).getOrCreate()

    val rdd:RDD[Row]= spark.sparkContext.textFile("data/data.txt")
      .map(_.split(","))
      .map(array => Row(array(0).trim, array(1).trim, array(2).trim,array(3).toInt))


    val coltype= StructType(
      StructField("peer_id",StringType, true) ::
        StructField("id_1", StringType, true) ::
        StructField("id_2", StringType, true) ::
        StructField("year", IntegerType, true):: Nil
    )

    val df: DataFrame = spark.createDataFrame(rdd, coltype)
    //显示 DataFrame
    df.show()
    //测试 DataFrame 的基本统计信息
    df.printSchema()
    df.createTempView("v_table")

    step1(spark).show()
    step2(spark).show()
    step3(spark).show()
    spark.stop()

    }

}
