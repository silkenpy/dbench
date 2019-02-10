package ir.rkr.dbench.MysqlConnector

import com.typesafe.config.Config
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import ir.rkr.dbench.utils.Metrics
import java.sql.Connection

class MysqlConnector(val config: Config, val metrics: Metrics) {


    val master = config.getString("master.name")
    val slave = config.getString("slave.name")

    val db = config.getString("db.name")
    val user = config.getString("db.user")
    val pwd = config.getString("db.password")

    val configMaster = HikariConfig()
            .apply {
                jdbcUrl = "jdbc:mysql://$master:3306/$db"
                username = user
                password = pwd
                driverClassName = "com.mysql.jdbc.Driver"
                maximumPoolSize = 1000
                minimumIdle = 100
            }

    val dsMaster = HikariDataSource(configMaster)


    val configSlave = HikariConfig()
            .apply {
                jdbcUrl = "jdbc:mysql://$slave:3306/$db"
                username = user
                password = pwd
                driverClassName = "com.mysql.jdbc.Driver"
                maximumPoolSize = 1000
                minimumIdle = 100
            }

    val dsSlave = HikariDataSource(configSlave)


    fun masterConnection(): Connection {
        return dsMaster.connection
    }


    fun slaveConnection(): Connection {
        return dsSlave.connection
    }


}