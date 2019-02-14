package ir.rkr.dbench

import com.google.common.base.Stopwatch
import com.typesafe.config.ConfigFactory
import ir.rkr.dbench.MysqlConnector.MysqlConnector
import ir.rkr.dbench.rest.JettyRestServer
import ir.rkr.dbench.utils.Metrics
import mu.KotlinLogging
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

const val version = 0.1


/**
 *  main entry point.
 */

fun main(args: Array<String>) {

    val logger = KotlinLogging.logger {}
    val config = ConfigFactory.defaultApplication()
    val metrics = Metrics()

    JettyRestServer(config, metrics)

    val mysql = MysqlConnector(config, metrics)

//    mysql.masterConnection().use { it ->
//        it.createStatement().execute("create table IF NOT EXISTS `test` (ID bigint, NAME varchar(50));")
//    }


    val numThreads = config.getInt("numThreads")
    val perThread = config.getInt("perThread")

    val stopwatch = Stopwatch.createStarted()
    val insertedToMaster = AtomicLong()
    val insertedToSlave = AtomicLong()
    val notInserted = AtomicLong()

    for (i in 1..numThreads) {
        thread {
            for (j in 1..perThread) {


                try {
                    mysql.masterConnection().use { connection ->

                        if (connection.isValid(50)) {
                            connection.prepareStatement("INSERT into test (ID,NAME) values ($i,\'salam $i\')").executeUpdate()
                            insertedToMaster.incrementAndGet()

                        } else {
                            throw Exception("Master Connection is not valid")
                        }
                    }
                } catch (e: Exception) {

                    try {
                        mysql.slaveConnection().use { connection ->
                            if (connection.isValid(50)) {
                                connection.prepareStatement("INSERT into test (ID,NAME) values ($i,\'salam $i\')").executeUpdate()
                                insertedToSlave.incrementAndGet()
                            } else {
                                throw Exception("Slave Connection is not valid")
                            }
                        }
                    } catch (e: Exception) {
                        notInserted.incrementAndGet()
                        println("InsertedToMaster: ${insertedToMaster.get()}  , InsertedToSlave ${insertedToSlave.get()} , notInserted : ${notInserted.get()} ")
                    }
                }
            }
        }
    }

    while ( (insertedToMaster.get() + insertedToSlave.get() + notInserted.get() ) != (numThreads * perThread).toLong()) {
        Thread.sleep(100)
    }
    println(stopwatch.elapsed(TimeUnit.MILLISECONDS))
    println("InsertedToMaster: ${insertedToMaster.get()}  , InsertedToSlave ${insertedToSlave.get()} , notInserted : ${notInserted.get()} ")
    println("Rate: ${(numThreads * perThread).toDouble() / stopwatch.elapsed(TimeUnit.SECONDS)}")
    System.exit(0)
}