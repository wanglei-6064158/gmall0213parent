package com.atguigu.gmall0213.realtime.util

import java.util

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}
import org.elasticsearch.index.query.MatchQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder

object MyEsUtil {

  var factory : JestClientFactory = null;
   def getJestClient: JestClient = {
     if(factory != null){
       factory.getObject
     }else {
       build()
       factory.getObject
     }
   }

  def build(): Unit ={
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200" )
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(1000).build())
  }

  //单条写入
  def saveToEs: Unit ={
    val jestClient: JestClient = getJestClient
    val index = new Index.Builder(Movie("32","朝花夕拾")).index("movie_test0213").`type`("_doc").id("34").build()
    jestClient.execute(index)
    jestClient.close()
  }

  //批次化操作
  def bulkSave(list:List[(Any,String)],indexName:String): Unit ={
    if(list != null && list.size > 0){
      val jestClient: JestClient = getJestClient
      val bulkBuilder = new Bulk.Builder
      bulkBuilder.defaultIndex(indexName).defaultType("_doc")
      for((doc,id) <- list)
      {
        val index: Index = new Index.Builder(doc).id(id).build()
        bulkBuilder.addAction(index)
      }
      val bulk: Bulk = bulkBuilder.build()
      val items: util.List[BulkResult#BulkResultItem] = jestClient.execute(bulk).getItems
      println("已保存"+items.size())
      jestClient.close()
    }
  }


  def queryFromEs: Unit ={
    val jestClient: JestClient = getJestClient
    val searchSourceBuilder = new SearchSourceBuilder
    searchSourceBuilder.query(new MatchQueryBuilder("name","red")).sort("doubanScore",SortOrder.DESC).from(0).size(20)
    val query2: String = searchSourceBuilder.toString
    println(query2)
    val query = "{\n  \"query\": {\n   \"match\": {\n     \"name\": \"red\"\n   }\n   \n  },\n  \"sort\":{\n    \"doubanScore\": {\n      \"order\": \"desc\"\n    }\n  },\n  \"from\": 0, \n  \"size\": 20\n}"
    val search: Search = new Search.Builder(query2).addIndex("movie_index").addType("movie").build()
    val result: SearchResult = jestClient.execute(search)
    val list: util.List[SearchResult#Hit[util.Map[String, Object], Void]] = result.getHits(classOf[util.Map[String,Object]])
    import collection.JavaConverters._
    for(hit <- list.asScala)
      {
        val source: util.Map[String, Object] = hit.source
        println(source)
      }

    jestClient.close()
  }

  def main(args: Array[String]): Unit = {
   // saveToEs
    queryFromEs
  }

  case class Movie(id:String,movie_name:String)

}
