package io.velum

import java.text.SimpleDateFormat
import java.util.Date

import com.dell.doradus.client.{Client, OLAPSession}
import com.dell.doradus.common._

import scala.util.Random


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
 * http://localhost:5711/vps/event/_aggregate?m=COUNT(eventType)&shards=s1
 *
 * http://localhost:5711/vps/event/_aggregate?m=DISTINCT(eventType)&shards=s1
 *
 * group equipment by time (minute buckets) and limit the time range
 *
 * http://localhost:5711/vps/event/_aggregate?m=COUNT(eventType)&shards=s1&f=TRUNCATE(timestamp, MINUTE),eventType&q=timestamp < "2015-06-03 20:00:00"
 *
 * Group Event Type counts by hour (this does it on the hour not a shifting window)
 * http://localhost:5711/vps/event/_aggregate?m=COUNT(eventType)&shards=s1&f=TRUNCATE(timestamp, HOUR),eventType *
 *
 * http://localhost:5711/vps/_shards/
 *
 * Merge shards by posting to
 * http://localhost:1123/vps/_shards/rack
 *
 *
 * Object Queries
 *
 * http://localhost:5711/vps/event/_query?range=0&q=version<3&o=timestamp+DESC
 *
 *
 */
object MainEvents extends App {

  // Serialize case class into Map
  def getCCParams(cc: AnyRef) =
    (Map[String, Any]() /: cc.getClass.getDeclaredFields) { (a, f) =>
      f.setAccessible(true)
      a + (f.getName -> f.get(cc))
    }

  try {

//    def ip: String = Random.nextInt(256) + "." + Random.nextInt(256) + "." + Random.nextInt(256) + "." + Random.nextInt(256)

    def createObj(tableName: String, id: String, items: Map[String, String]): DBObject = {
      val obj = new DBObject(id, tableName)
      items.foreach {
        case (key, value) =>
          obj.addFieldValue(key, value)
      }
      obj
    }

    val client = new Client("localhost", 5711)

    val app = client.createApplication(
      """{"vps": {
        |       "key": "name",
        |       "options": { "StorageService": "OLAPService" },
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
    //          "sharding-field": "time",
    //  "sharding-granularity": "DAY"
    // ,
    //  "cacheBlock": {"type": "XLINK", "table": "cacheBlock", "inverse": "rack", "junction": "_ID"}


    //,
    //"rack": {"type": "XLINK", "table": "rack", "inverse": "cacheBlock", "junction": "_ID"}


    val vps = client.openApplication("vps").asInstanceOf[OLAPSession]

    var i = 0

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    val rackBatch = new DBObjectBatch()
    val cacheBlockBatch = new DBObjectBatch()

    (0 to 1000000 * 1000).foreach {
      l =>
        //        val psuBlockBatch = new DBObjectBatch()
        //        val psusBatch = new DBObjectBatch()
        //        val cacheBatch = new DBObjectBatch()

        val _ID = java.util.UUID.randomUUID.toString

        rackBatch.addObject(
          createObj("event", _ID, Map(
//            "id" -> java.util.UUID.randomUUID.toString,
            "timestamp" -> format.format(System.currentTimeMillis()),
            "eventType" -> Random.shuffle(List("State", "UserAction", "EquipChange", "Fault")).head,
            "version" -> Random.nextInt(10).toString,
            "msg" -> Random.shuffle(List("wi tu lo", "deng dong ding", "crash bum bang", "oink")).head,
            "entity" -> java.util.UUID.randomUUID.toString,
            "entityType" -> Random.shuffle(List("PSU", "Battery", "SuperCap")).head, //s"${Random.nextInt(10)}.${Random.nextInt(10)}.${Random.nextInt(10)}"
            "oldValue" -> format.format(new Date(System.currentTimeMillis() - Math.abs(Random.nextInt())))
          )
          )
        )

        //        rackBatch.addObject(
        //          createObj("cacheBlock", "s1", _ID, Map(
        //            "state" -> Random.shuffle(List("ACTIVE", "OFFLINE", "SUSPENDED")).head,
        //            "host" -> s"http://${ip}:8080",
        //            "modelNumber" -> Random.shuffle(List("Model-V", "Model-A", "Model-B", "Model-C")).head,
        //            "manufacturer" -> Random.shuffle(List("Artesyn", "VPS", "StackVelocity", "enArka")).head,
        //            "serial" -> Random.nextLong().toString,
        //            "firmwareVer" -> s"${Random.nextInt(10)}.${Random.nextInt(10)}.${Random.nextInt(10)}",
        //            "manufacturerDate" -> format.format(new Date(System.currentTimeMillis() - Math.abs(Random.nextInt())))
        //          )
        //          )
        //        )

        //        val
        //        cacheBlock = new DBObject()
        //        val cBID = l + ":c:" + java.util.UUID.randomUUID.toString
        //        cacheBlock.addFieldValue("_ID", cBID)
        //        cacheBlock.addFieldValue("maxCacheSlots", 5.toString)
        //        cacheBlock.addFieldValue("modelNumber", Random.shuffle(
        //          List("Model-V", "Model-A", "Model-B", "Model-C")).head)
        //        cacheBlock.addFieldValue("manufacturer", Random.
        //          shuffle(List("Artesyn", "VPS", "StackVelocity", "enArka")).head)
        //        cacheBlock.addFieldValue("serial", Random
        //          .nextLong().toString)
        //        cacheBlock.addFieldValue(
        //          "firmwareVer", s"${Random.nextInt(10)}.${Random.nextInt(10)}.${Random.nextInt(10)}")
        //        //      cacheBlock.addFieldValue(
        //        //        "rack", _ID)
        //        cacheBlock.setTableName("cacheBlock")
        //        cacheBlock.setShardName("rack")
        //        cacheBlockBatch.addObject(cacheBlock)

        i += 1

        if (i % 100 == 0) {
          val rackRes = vps.addBatch("s1", rackBatch)
          //          val
          //          cacheBlockRes = vps.addBatch("cacheBlock", cacheBlockBatch)
          println(i + " " + rackRes.getErrorMessage)
          println(rackRes.getFailedObjectIDs)
          //          println(i + " " + cacheBlockRes.getErrorMessage)
          //          println(cacheBlockRes.getFailedObjectIDs)
        }

        if (i % 600 == 0) {
          println("Merging...")
          vps.mergeShard("s1", null)
          //          vps.mergeShard("cacheBlock", null)
          rackBatch.clear()
          cacheBlockBatch.clear()
        }

    }

  } catch {

    case e: Exception =>
      e.printStackTrace()
  }

  println("Done!")

}
