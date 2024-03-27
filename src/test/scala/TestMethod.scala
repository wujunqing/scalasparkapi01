package scala


import com.myteat.ScalaSparkDataFrameApi
import org.scalatest.flatspec.AnyFlatSpec

import java.util.NoSuchElementException
import scala.collection.immutable.Set




class TestMethod extends AnyFlatSpec {
  val api = new ScalaSparkDataFrameApi()
  var sql = String.format("""
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
  val list = api.sqlApi("data/data.txt",sql)

  behavior of "list have 3 row"
    it should "list.size have 3" in {
      assert(list.size === 3)
    }
    it should  "list don't have 3 row" in {
        assertThrows[Exception] {
          "list.size is :"+list.size
        }
    }




}