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

    mysql.masterConnection().use { it ->
        it.createStatement().execute("create table IF NOT EXISTS `test` (ID bigint, NAME varchar(50));")
    }


    val stopwatch = Stopwatch.createStarted()

    val selected = AtomicLong()
    for (i in 1..100) {
        thread {

            mysql.masterConnection().use { connection ->

                for (j in 1..10000) {

                    try {
                        connection.prepareStatement("INSERT into test (ID,NAME) values ($i,\'salam $i\')").execute()
                        selected.incrementAndGet()

                    } catch (e: Exception) {
                        mysql.slaveConnection().use { connection ->
                            connection.prepareStatement("INSERT into test (ID,NAME) values ($i,\'salam $i\')").execute()
                            selected.incrementAndGet()

                        }
                    }
                }

            }
        }
    }

    while (selected.get() != 2000000L) {
     Thread.sleep(100)
    }
    println(stopwatch.elapsed(TimeUnit.MILLISECONDS))
    System.exit(0)
}