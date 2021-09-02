package ketl

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

fun sqliteDatasource(
  dbPath: String = "./etl.db",
  driverClassName: String = "org.sqlite.JDBC"
): HikariDataSource {
  val config = HikariConfig()
  config.jdbcUrl = "jdbc:sqlite:$dbPath"
  config.driverClassName = driverClassName
  config.maximumPoolSize = 1
  config.transactionIsolation = "TRANSACTION_SERIALIZABLE"
  config.addDataSourceProperty("cachePrepStmts", "true")
  config.addDataSourceProperty("prepStmtCacheSize", "250")
  config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
  return HikariDataSource(config)
}

fun pgDatasource(
  url: String, // eg, "jdbc:postgresql://host/dbname"
  username: String,
  password: String,
  maximumPoolSize: Int = 5,
  idleTimeoutMillis: Long = 900_000, // 15 minutes
  maxLifetimeMillis: Long = 3600_000, // 1 hour
  connectionTimeoutMillis: Long = 1800_000, // 30 minutes
  driverClassName: String = "org.postgresql.Driver",
): HikariDataSource {
  val config = HikariConfig()
  config.jdbcUrl = url
  config.driverClassName = driverClassName
  config.username = username
  config.password = password
  config.maximumPoolSize = maximumPoolSize
  config.idleTimeout = idleTimeoutMillis
  config.maxLifetime = maxLifetimeMillis
  config.connectionTimeout = connectionTimeoutMillis
  config.addDataSourceProperty("cachePrepStmts", "true")
  config.addDataSourceProperty("prepStmtCacheSize", "250")
  config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
  return HikariDataSource(config)
}
