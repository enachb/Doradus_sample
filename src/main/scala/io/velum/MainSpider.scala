package io.velum


import java.io.StringReader
import java.text.SimpleDateFormat
import java.util.{Date, Scanner}

import com.dell.doradus.client.Client
import com.dell.doradus.common._
import org.joda.time.format.DateTimeFormat

import play.api.libs.json._

import scala.util.Random


/**
 * Created by erich on 2/22/15.
 */
object MainSpider extends App{

  val client = new Client("localhost", 1123)

  val app = client.createApplication(
    """{
      |   "VPS": {
      |       "key": "id",
      |       "options": { "StorageService": "SpiderService" },
      |       "tables": {
      |         "Event": {
      |           "options": {},
      |           "fields": {
      |               "idXXXXXX": {"type": "TEXT" },
      |               "timestamp": {"type": "TIMESTAMP" },
      |               "eventType": {"type": "TEXT"},
      |               "version": {"type": "FLOAT"},
      |               "msg": {"type": "TEXT"},
      |               "entity": {"type": "TEXT"},
      |               "entityType": {"type": "TEXT"},
      |               "oldValue": {"type": "TEXT"},
      |               "newValue": {"type": "TEXT"},
      |               "action": {"type": "TEXT"},
      |               "user": {"type": "TEXT"},
      |               "added": {"type": "TEXT"},
      |               "removed": {"type": "TEXT"},
      |               "faultDesc": {"type": "TEXT"},
      |               "faultCode": {"type": "TEXT"},
      |               "severity": {"type": "INTEGER"}
      |            }
      |         }
      |       }
      |  }
      |}""".stripMargin, ContentType.APPLICATION_JSON)

  val events = client.openApplication("VPS")

  val batch = new DBObjectBatch()

  var j = 0

  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  (0 to 1000000 * 1000).foreach {
    i =>
      val obj = new DBObject()

      obj.addFieldValue("timestamp", format.format(System.currentTimeMillis()))
      obj.addFieldValue("eventType", Random.shuffle(List("State", "UserAction", "EquipChange", "Fault")).head)
      obj.addFieldValue("version", Random.nextInt(10).toString)
      obj.addFieldValue("msg", Random.shuffle(List("wi tu lo", "deng dong ding", "crash bum bang", "oink")).head)
      obj.addFieldValue("entity", java.util.UUID.randomUUID.toString)
      obj.addFieldValue("entityType", Random.shuffle(List("PSU", "Battery", "SuperCap")).head)
      obj.addFieldValue("oldValue", format.format(new Date(System.currentTimeMillis() - Math.abs(Random.nextInt()))))

      batch.addObject(obj)

      j += 1
      if(i % 10000 == 0) {
        val res = events.addBatch("fbComments", batch)
        println(i + " " + res.getErrorMessage)
        println(res.getFailedObjectIDs)
      }
  }
}
