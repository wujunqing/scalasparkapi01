import com.myteat.ScalaSparkDataFrameApi

object TestSpark {



  def main(args: Array[String]): Unit = {


    //    1. For each peer_id, get the year when peer_id contains id_2, for example for ‘ABC17969(AB)’ year is 2022.
    //    使用 INSTR 函数和 when 表达式来检查 "peer_id" 字段中是否包含 "id_2"
    //    如果包含，则标记为 "Yes"，否则标记为 "No"
    val step1sql: String =
      """
        |select peer_id
        |      ,id_1
        |      ,id_2
        |      ,year
        |      ,if(instr(peer_id,id_2)>0,year,null) v_year
        |   from v_table
        |""".stripMargin

    //  2.	Given a size number, for example 3. For each peer_id count the number of each year (which is smaller or equal than the year in step1).
    //    For example, for ‘ABC17969(AB)’, the count should be:
    // Given a size number, for example 3
    val  step2sql: String="""
        |select year,count(*) v_count
        |from (select peer_id
        |      ,year
        |      ,max(if(instr(peer_id,id_2)>0,year,null)) over(partition by peer_id) v_year
        |   from v_table
        |   ) a
        |     where v_year>=year
        |     group by peer_id,year
        |""".stripMargin

    //    3.	Order the value in step 2 by year and check if the count number of the first year is bigger or equal than the given size number. If yes, just return the year.
    //    If not, plus the count number from the biggest year to next year until the count number is bigger or equal than the given number. For example, for ‘AE686(AE)’, the year is 2023, and count are:

    var step3sql: String=String.format("""
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
                      """, "3")

    val api = new ScalaSparkDataFrameApi()
    val step1 = api.sqlApi("data/data.txt", step1sql)
    println("step1 is ====")
    step1.foreach(println)
    val step2 = api.sqlApi("data/data.txt", step2sql)
    println("step2 is ====")
    step2.foreach(println)
    println("step3 is ====")
    val step3 = api.sqlApi("data/data.txt", step3sql)
    step3.foreach(println)

    }

}
