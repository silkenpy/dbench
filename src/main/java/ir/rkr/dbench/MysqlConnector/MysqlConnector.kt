package ir.rkr.dbench.MysqlConnector

import com.typesafe.config.Config
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import ir.rkr.dbench.utils.Metrics
import java.sql.Connection

class MysqlConnector(val config: Config, val metrics: Metrics) {


    val master = config.getString("master.name")
    val slave = config.getString("slave.name")

    val jdbcUrlMaster = config.getString("db.jdbcUrlMaster")
    val jdbcUrlSlave = config.getString("db.jdbcUrlSlave")
    val db = config.getString("db.name")
    val user = config.getString("db.user")
    val pwd = config.getString("db.password")
    val className = config.getString("db.className")

    val configMaster = HikariConfig()
            .apply {
//                jdbcUrl = "jdbc:mysql://$master:3306/$db"
//                jdbcUrl = "jdbc:mariadb:loadbalance//$master,$slave:3306/$db"
//                driverClassName = "com.mysql.jdbc.Driver"

                jdbcUrl = "$jdbcUrlMaster/$db"
                username = user
                password = pwd
                driverClassName = className
                maximumPoolSize = 1000
                minimumIdle = 100
                connectionTimeout = 250
                validationTimeout = 250
                idleTimeout=10000

            }

    val dsMaster = HikariDataSource(configMaster)

    val configSlave = HikariConfig()
            .apply {
//                jdbcUrl = "jdbc:mysql://$slave:3306/$db"
//                driverClassName = "com.mysql.jdbc.Driver"
//                driverClassName = "org.mariadb.jdbc.Driver"
                jdbcUrl = "$jdbcUrlSlave/$db"
                username = user
                password = pwd
                driverClassName = className
                maximumPoolSize = 1000
                minimumIdle = 100
                connectionTimeout = 250
                validationTimeout = 250
                idleTimeout=10000


            }

    val dsSlave = HikariDataSource(configSlave)


    fun masterConnection(): Connection {

        return dsMaster.connection
    }


    fun slaveConnection(): Connection {
        return dsSlave.connection
    }


}