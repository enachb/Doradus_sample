package io.velum


import java.io.StringReader
import java.text.SimpleDateFormat
import java.util.{Date, Scanner}

import com.dell.doradus.client.Client
import com.dell.doradus.common._
import org.joda.time.format.DateTimeFormat

import play.api.libs.json._


/**
 * Created by erich on 2/22/15.
 */
object MainSpider extends App{

  val client = new Client("localhost", 1123)

  val app = client.createApplication(
    """{"fb": {
      |       "key": "id",
      |       "options": { "StorageService": "SpiderService" },
      |"tables": {
      |   "fbComments": {
      |       "options": {
      |       },
      |       "fields": {
      |          "pageUrl": {"type": "TEXT", "analyzer": "OpaqueTextAnalyzer"},
      |          "text": {"type": "TEXT" },
      |          "likes": {"type": "INTEGER"},
      |          "fromId": {"type": "TEXT", "analyzer": "OpaqueTextAnalyzer"},
      |          "id": {"type": "TEXT", "analyzer": "OpaqueTextAnalyzer"},
      |          "postId": {"type": "TEXT", "analyzer": "OpaqueTextAnalyzer"},
      |          "postVersion": {"type": "TEXT", "analyzer": "OpaqueTextAnalyzer"},
      |          "time": {"type": "TIMESTAMP"}
      |        }
      |      }
      |    }
      |  }
      |}""".stripMargin, ContentType.APPLICATION_JSON)

  //          "sharding-field": "time",
//  "sharding-granularity": "DAY"

  val fbComments = client.openApplication("fb")


  val batch = new DBObjectBatch()

  var i = 0

  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  scala.io.Source.fromFile("/Volumes/enachb/quantifind/fb_and_tweets/fb/fbComment/fbComment-378233.dmp").getLines().foreach{
    l =>
//      println(s"JSON: ${l}")
      val json = Json.parse(l)
      val obj = new DBObject()
      obj.addFieldValue("pageUrl", (json \ "pageUrl").as[String])
      obj.addFieldValue("text", (json \ "text").as[String])
      obj.addFieldValue("likes", (json \ "likes").as[Int].toString)
      obj.addFieldValue("fromId", (json \ "id").as[String])
      obj.addFieldValue("_ID", (json \ "id").as[String])
      obj.addFieldValue("postId", (json \ "id").as[String])
      obj.addFieldValue("postVersion", (json \ "id").as[String])
      obj.addFieldValue("time", format.format(new Date((json \ "time").as[Long])))

      batch.addObject(obj)

      i += 1
      if(i % 10000 == 0) {
        val res = fbComments.addBatch("fbComments", batch)
        println(i + " " + res.getErrorMessage)
        println(res.getFailedObjectIDs)
      }

  }



}
