package io.velum

import java.text.SimpleDateFormat
import java.util.Date

import com.dell.doradus.client.{Client, OLAPSession}
import com.dell.doradus.common._

import scala.util.Random


/**
 * OLAP
 *
 *
 * Sample
 *
 * Distinct States
 * http://localhost:1123/vps/rack/_aggregate?m=COUNT(state)&f=state&shards=rack
 *
 * http://localhost:1123/vps/rack/_aggregate?m=COUNT(firmwareVer)&f=firmwareVer&shards=rack
 *
 * http://localhost:1123/vps/_shards/
 *
 * Merge shards by posting to
 * http://localhost:1123/vps/_shards/rack
 *
 */
object MainOlap extends App {

  try {

    def ip: String = Random.nextInt(256) + "." + Random.nextInt(256) + "." + Random.nextInt(256) + "." + Random.nextInt(256)

    def createObj(tableName: String, shardName: String, id: String, items: Map[String, String]): DBObject = {
      val obj = new DBObject(id,tableName)
      obj.setShardName(shardName)
      items.foreach {
        case (key, value) =>
          obj.addFieldValue(key, value)
      }
      obj
    }

    val client = new Client("localhost", 1123)

    val app = client.createApplication(
      """{"vps": {
        |       "key": "name",
        |       "options": { "StorageService": "OLAPService" },
        |"tables": {
        |   "rack": {
        |       "options": {
        |       },
        |       "fields": {
        |          "host": {"type": "TEXT" },
        |          "modelNumber": {"type": "TEXT" },
        |          "manufacturer": {"type": "TEXT"},
        |          "serial": {"type": "TEXT"},
        |          "firmwareVer": {"type": "TEXT"},
        |          "manufactureDate": {"type": "TIMESTAMP"},
        |          "state": {"type": "TEXT"}
        |        }
        |      },
        |       "psuBlock": {
        |       "options": {
        |       },
        |       "fields": {
        |          "name": {"type": "TEXT"},
        |          "maxPsuSlots": {"type": "INTEGER" },
        |          "maxUtilSlots": {"type": "INTEGER" },
        |          "maxOutputCurrentAmp": {"type": "INTEGER" },
        |          "modelNumber": {"type": "TEXT" },
        |          "manufacturer": {"type": "TEXT"},
        |          "serial": {"type": "TEXT"},
        |          "firmwareVer": {"type": "TEXT"},
        |          "manufactureDate": {"type": "TIMESTAMP"}
        |        }
        |      },
        |      "cacheBlock": {
        |       "options": {
        |       },
        |       "fields": {
        |          "maxCacheSlots": {"type": "INTEGER" },
        |          "maxOutputCurrentAmp": {"type": "INTEGER" },
        |          "modelNumber": {"type": "TEXT" },
        |          "manufacturer": {"type": "TEXT"},
        |          "serial": {"type": "TEXT"},
        |          "firmwareVer": {"type": "TEXT"},
        |          "manufactureDate": {"type": "TIMESTAMP"}
        |        }
        |      },
        |      "cache": {
        |       "options": {
        |       },
        |       "fields": {
        |          "name": {"type": "TEXT"},
        |          "maxVoltageV": {"type": "INTEGER" },
        |          "minVoltageV": {"type": "INTEGER" },
        |          "maxCurrentAmp": {"type": "INTEGER" },
        |          "maxTemperatureC": {"type": "INTEGER" },
        |          "maxPowerW": {"type": "INTEGER" },
        |          "estimatedMtbf": {"type": "INTEGER" },
        |          "maxCycles": {"type": "INTEGER" },
        |          "ratedChargeTimeMins": {"type": "INTEGER" },
        |          "maxCapacityWh": {"type": "INTEGER" },
        |          "maxChargeRateW": {"type": "INTEGER" },
        |          "chargeEfficiencyProp": {"type": "FLOAT" },
        |          "modelNumber": {"type": "TEXT" },
        |          "manufacturer": {"type": "TEXT"},
        |          "serial": {"type": "TEXT"},
        |          "firmwareVer": {"type": "TEXT"},
        |          "manufactureDate": {"type": "TIMESTAMP"},
        |          "slot": {"type": "INTEGER" }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin, ContentType.
        APPLICATION_JSON)
    //          "sharding-field": "time",
    //  "sharding-granularity": "DAY"
    // ,
    //  "cacheBlock": {"type": "XLINK", "table": "cacheBlock", "inverse": "rack", "junction": "_ID"}


    //,
    //"rack": {"type": "XLINK", "table": "rack", "inverse": "cacheBlock", "junction": "_ID"}


    val vps = client.openApplication("vps").asInstanceOf[OLAPSession]

    var i = 0

    val format = new
        SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    val rackBatch = new DBObjectBatch()
    val cacheBlockBatch = new DBObjectBatch()

    (0 to 1000000*1000).foreach {
      l =>
        //        val psuBlockBatch = new DBObjectBatch()
        //        val psusBatch = new DBObjectBatch()
        //        val cacheBatch = new DBObjectBatch()

        val _ID = java.util.UUID.randomUUID.toString

        rackBatch.addObject(
          createObj("rack", "s1", _ID, Map(
            "state" -> Random.shuffle(List("ACTIVE", "OFFLINE", "SUSPENDED")).head,
            "host" -> s"http://${ip}:8080",
            "modelNumber" -> Random.shuffle(List("Model-V", "Model-A", "Model-B", "Model-C")).head,
            "manufacturer" -> Random.shuffle(List("Artesyn", "VPS", "StackVelocity", "enArka")).head,
            "serial" -> Random.nextLong().toString,
            "firmwareVer" -> s"${Random.nextInt(10)}.${Random.nextInt(10)}.${Random.nextInt(10)}",
            "manufacturerDate" -> format.format(new Date(System.currentTimeMillis() - Math.abs(Random.nextInt())))
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

        if (i % 10000 == 0) {
          val rackRes = vps.addBatch("s1", rackBatch)
//          val
//          cacheBlockRes = vps.addBatch("cacheBlock", cacheBlockBatch)
          println(i + " " + rackRes.getErrorMessage)
          println(rackRes.getFailedObjectIDs)
//          println(i + " " + cacheBlockRes.getErrorMessage)
//          println(cacheBlockRes.getFailedObjectIDs)
        }

        if (i % 50000 == 0) {
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
