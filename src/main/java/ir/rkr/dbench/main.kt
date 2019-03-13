package ir.rkr.dbench

import com.google.common.base.Stopwatch
import com.google.common.util.concurrent.RateLimiter
import com.typesafe.config.ConfigFactory
import ir.rkr.dbench.MysqlConnector.MysqlConnector
import ir.rkr.dbench.rest.JettyRestServer
import ir.rkr.dbench.utils.Metrics
import mu.KotlinLogging
import org.json.JSONObject
import java.sql.ResultSet
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

const val version = 0.1


fun resultsetToJson(rs: ResultSet): String {

//    val result = JSONObject()

    val result = StringBuilder()
//    result.append("CREATE TABLE TRAFFIC(id BIGINT, net INT, mediaType INT, sent BIGINT, received BIGINT, sentByte BIGINT," +
//            " receivedByte BIGINT, sentPerDay BIGINT, receivedPerDay BIGINT, t TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(id, net, mediaType) );\n")
//    var count = 0
    while (rs.next()) {

        val numColumns = rs.metaData.getColumnCount()
        val obj = JSONObject()

        for (i in 1 until numColumns + 1) {
            val column_name = rs.metaData.getColumnName(i)

            when (rs.metaData.getColumnType(i)) {
                java.sql.Types.ARRAY -> obj.put(column_name, rs.getArray(column_name))
                java.sql.Types.BIGINT -> obj.put(column_name, rs.getInt(column_name))
                java.sql.Types.BOOLEAN -> obj.put(column_name, rs.getBoolean(column_name))
                java.sql.Types.BLOB -> obj.put(column_name, rs.getBlob(column_name))
                java.sql.Types.DOUBLE -> obj.put(column_name, rs.getDouble(column_name))
                java.sql.Types.FLOAT -> obj.put(column_name, rs.getFloat(column_name))
                java.sql.Types.INTEGER -> obj.put(column_name, rs.getInt(column_name))
                java.sql.Types.NVARCHAR -> obj.put(column_name, rs.getNString(column_name))
                java.sql.Types.VARCHAR -> obj.put(column_name, rs.getString(column_name))
                java.sql.Types.TINYINT -> obj.put(column_name, rs.getInt(column_name))
                java.sql.Types.SMALLINT -> obj.put(column_name, rs.getInt(column_name))
                java.sql.Types.DATE -> obj.put(column_name, rs.getDate(column_name))
                java.sql.Types.TIMESTAMP -> obj.put(column_name, rs.getTimestamp(column_name))
                else -> obj.put(column_name, rs.getObject(column_name))
            }
        }
        result.append("\n" + obj.toString())
        // println(obj.toString())
//        result.put(count.toString(), obj.toString())
//        count += 1
    }

    return result.toString()
}

/**
 *  main entry point.
 */

fun main(args: Array<String>) {

    val logger = KotlinLogging.logger {}
    val config = ConfigFactory.defaultApplication()
    val metrics = Metrics()
    val writeRate = RateLimiter.create(config.getDouble("writeRateLimit"))
    val readRate  = RateLimiter.create(config.getDouble("readRateLimit"))
    val sleepTime  = config.getLong("sleepTime")


    JettyRestServer(config, metrics)

    val mysql = MysqlConnector(config, metrics)
    Thread.sleep(1000)

//    mysql.masterConnection().use { it ->
//        it.createStatement().execute("create table IF NOT EXISTS `test` (ID bigint, NAME varchar(50));")
//    }


    val numThreads = config.getInt("numThreads")
    val perThread = config.getInt("perThread")
    val startFrom = config.getInt("startFrom")
    val mode = config.getString("mode")
    val sqlType = config.getString("sqlType")
    val tableName = config.getString("tableName")


    val stopwatch = Stopwatch.createStarted()
    val sqlOnMaster = AtomicLong()
    val sqlOnSlave = AtomicLong()
    val notExecuted = AtomicLong()

    if (mode == "write")
        for (i in 1..numThreads) {
            thread {
                for (j in startFrom + 1..startFrom + perThread) {

                    if (!writeRate.tryAcquire()) Thread.sleep(sleepTime)

                    try {
                        mysql.masterConnection().use { connection ->

                            if (connection.isValid(50)) {
                                if (sqlType == "mysql")
                                    connection.prepareStatement("insert into $tableName (`userId`,`peerId`,`randomId`,`messageId`,`date`,`message`,`out`,`pts`,`media`,`deleted`,`fwdFrom`,`editDate`,`replyTo`) " +
                                            "values($i,$i,$j,$j, ${System.currentTimeMillis() / 1000} ,\'salam $j\', 1 , $j , 5,0,0,0,0);").executeUpdate()
                                else
                                    connection.prepareStatement("insert into $tableName (userId,peerId,randomId,messageId,date,message,out,pts,media,deleted,fwdFrom,editDate,replyTo) " +
                                            "values($i,$i,$j,$j, ${System.currentTimeMillis() / 1000} ,\'salam $j\', 1 , $j , \'5\',0,0,0,0);").executeUpdate()

                                sqlOnMaster.incrementAndGet()

                            } else {
                                throw Exception("Master Connection is not valid")
                            }
                        }
                    } catch (e: Exception) {

                        try {
                            mysql.slaveConnection().use { connection ->
                                if (connection.isValid(50)) {
                                    if (sqlType == "mysql")
                                        connection.prepareStatement("insert into $tableName (`userId`,`peerId`,`randomId`,`messageId`,`date`,`message`,`out`,`pts`,`media`,`deleted`,`fwdFrom`,`editDate`,`replyTo`) " +
                                                "values($i,$i,$j,$j, ${System.currentTimeMillis() / 1000} ,\'salam $j\', 1 , $j , 5,0,0,0,0);").executeUpdate()
                                    else
                                        connection.prepareStatement("insert into $tableName (userId,peerId,randomId,messageId,date,message,out,pts,media,deleted,fwdFrom,editDate,replyTo) " +
                                                "values($i,$i,$j,$j, ${System.currentTimeMillis() / 1000} ,\'salam $j\', 1 , $j , \'5\',0,0,0,0);").executeUpdate()

                                    sqlOnSlave.incrementAndGet()
                                } else {
                                    throw Exception("Slave Connection is not valid")
                                }
                            }
                        } catch (e: Exception) {
                            notExecuted.incrementAndGet()
                            println("SqlOnMaster: ${sqlOnMaster.get()}  , SqlOnSlave ${sqlOnSlave.get()} , notExecuted : ${notExecuted.get()} ")
                        }
                    }
                }
            }
        }


    if (mode == "read")
        for (i in 1..numThreads) {
            thread {
                for (j in startFrom + 1..startFrom + perThread) {

                    if (!readRate.tryAcquire()) Thread.sleep(sleepTime)

                    try {
                        mysql.masterConnection().use { connection ->
                            if (connection.isValid(100)) {
                                if (sqlType == "mysql")
                                    connection.prepareStatement("select * from  $tableName where `userId`=$i and peerId=$i and `randomId`=$j and `messageId`=$j limit 10;").executeQuery()
                                else
                                    connection.prepareStatement("select * from  $tableName where userId=$i and peerId=$i and randomId=$j and messageId=$j limit 10;").executeQuery()
                                sqlOnMaster.incrementAndGet()

//                                logger.debug { resultsetToJson(res) }

                            } else {
                                throw Exception("Master Connection is not valid")
                            }
                        }
                    } catch (e: Exception) {
                        notExecuted.incrementAndGet()
                        println(e)

                    }


                }
            }
        }


    while ((sqlOnMaster.get() + sqlOnSlave.get() + notExecuted.get()) != (numThreads * perThread).toLong()) {
        Thread.sleep(100)
    }


    println(stopwatch.elapsed(TimeUnit.MILLISECONDS))
    println("SqlOnMaster: ${sqlOnMaster.get()}  , SqlOnSlave ${sqlOnSlave.get()} , notExecuted : ${notExecuted.get()} ")
    println("Rate: ${(numThreads * perThread).toDouble() / stopwatch.elapsed(TimeUnit.SECONDS)}")
    System.exit(0)
}