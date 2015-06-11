package io.velum

import java.text.SimpleDateFormat
import java.util.Date

import com.dell.doradus.client.{Client, SpiderSession}
import com.dell.doradus.common._

import scala.util.{Try, Random}


/**
 * OLAP
 *
 * Start Doradus
 *
 *
 * java -Xmx2G -Xms2G -cp ./doradus.yaml:$(find . -iname *jar | tr '\n' ':') com.dell.doradus.core.DoradusServer -restport 5711
 *
 * Sample
 *
 * Distinct States (for all queries to work the content-type has to be application/json)
 *
 * http://localhost:5711/vps/event/_aggregate?m=COUNT(eventType)
 *
 * http://localhost:5711/vps/event/_aggregate?m=DISTINCT(eventType)
 *
 * group equipment by time (minute buckets) and limit the time range
 *
 * http://localhost:5711/vps/event/_aggregate?m=COUNT(eventType)&f=TRUNCATE(timestamp, MINUTE),eventType&q=timestamp > "2015-06-03 20:00:00"
 *
 * Group Event Type counts by hour (this does it on the hour not a shifting window)
 * http://localhost:5711/vps/event/_aggregate?m=COUNT(eventType)&shards=s1&f=TRUNCATE(timestamp, HOUR),eventType
 *
 *
 * Object Queries
 *
 * http://localhost:5711/vps/event/_query?range=0&q=version<3&o=timestamp+DESC
 *
 *
 */
object MainEventsSpider extends App {

  val client = new Client("localhost", 1123)

  // Serialize case class into Map
  def getCCParams(cc: AnyRef) =
    (Map[String, Any]() /: cc.getClass.getDeclaredFields) { (a, f) =>
      f.setAccessible(true)
      a + (f.getName -> f.get(cc))
    }

  def createObj(tableName: String, id: String, items: Map[String, String]): DBObject = {
    val obj = new DBObject(id, tableName)
    items.foreach {
      case (key, value) =>
        obj.addFieldValue(key, value)
    }
    obj
  }

  def setup(client: Client) = {
    client.createApplication(
      """{"vps": {
        |       "key": "name",
        |       "options": { "StorageService": "SpiderService" },
        |"tables": {
        |   "event": {
        |       "options": {
        |       },
        |       "fields": {
        |          "idXXXXXX": {"type": "TEXT" },
        |          "timestamp": {"type": "TIMESTAMP" },
        |          "eventType": {"type": "TEXT"},
        |          "version": {"type": "FLOAT"},
        |          "msg": {"type": "TEXT"},
        |          "entity": {"type": "TEXT"},
        |          "entityType": {"type": "TEXT"},
        |          "oldValue": {"type": "TEXT"},
        |          "newValue": {"type": "TEXT"},
        |          "action": {"type": "TEXT"},
        |          "user": {"type": "TEXT"},
        |          "added": {"type": "TEXT"},
        |          "removed": {"type": "TEXT"},
        |          "faultDesc": {"type": "TEXT"},
        |          "faultCode": {"type": "TEXT"},
        |          "severity": {"type": "INTEGER"}
        |        }
        |      },
        |       "equipment": {
        |       "options": {
        |       },
        |       "fields": {
        |          "id": {"type": "TEXT"},
        |          "equipType": {"type": "TEXT" },
        |          "location": {"type": "TEXT" }
        |        }
        |      }
        |    }
        |  }
        |}
        | """.stripMargin, ContentType.
        APPLICATION_JSON)
  }
  
  def spiderTestCaseRunner(): Unit = {
    setup(client)
    
    val vps = client.openApplication("vps").asInstanceOf[SpiderSession]

    val res0 = loadEntriesTest(vps)
    println(res0)

    val res1 = addPayloadWithFieldsNotInSchema(vps)
    println(res1)

    val res2 = addPayloadWithIncorrectPayloadType(vps)
    println(res2)
  }

  def addPayloadWithIncorrectPayloadType(vps: SpiderSession) = {
    Try {
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      (0 to 100).foreach {
        l =>
          val _ID = java.util.UUID.randomUUID.toString

          vps.addObject("event",
            createObj("event", _ID, Map(
              "timestamp" -> format.format(System.currentTimeMillis()),
              "severity" -> java.util.UUID.randomUUID.toString
            )))
      }
    }
  }

  def addPayloadWithFieldsNotInSchema(vps: SpiderSession) = {
    Try {
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      (0 to 100).foreach {
        l =>
          val _ID = java.util.UUID.randomUUID.toString

          val f1 = _ID
          val f2 = s"$l"

          vps.addObject("event",
            createObj("event", _ID, Map(
              "timestamp" -> format.format(System.currentTimeMillis()),
              "eventType" -> Random.shuffle(List("State", "UserAction", "EquipChange", "Fault")).head,
              f1 -> java.util.UUID.randomUUID.toString,
              f2 -> l.toString
            )))
      }
    }
  }

  //Load a million entries into spider without an issue
  def loadEntriesTest(vps: SpiderSession, entryCount: Int = 100000) = {
    Try {
      var i = 0

      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      (0 to entryCount).foreach {
        l =>

          val _ID = java.util.UUID.randomUUID.toString

          val objRes = vps.addObject("event",
            createObj("event", _ID, Map(
              "timestamp" -> format.format(System.currentTimeMillis()),
              "eventType" -> Random.shuffle(List("State", "UserAction", "EquipChange", "Fault")).head,
              "version" -> Random.nextInt(10).toString,
              "msg" -> Random.shuffle(List("wi tu lo", "deng dong ding", "crash bum bang", "oink")).head,
              "entity" -> java.util.UUID.randomUUID.toString,
              "entityType" -> Random.shuffle(List("PSU", "Battery", "SuperCap")).head,
              "oldValue" -> format.format(new Date(System.currentTimeMillis() - Math.abs(Random.nextInt())))
            )
            )
          )

          i += 1

          if (i % 100 == 0) {
            println(i + " " + objRes.getErrorMessage)
          }
      }
    }
  }

  def purgeApplication() = {
    client.deleteApplication("vps", "name")
    println("Deleted application")
  }

  spiderTestCaseRunner()
}

