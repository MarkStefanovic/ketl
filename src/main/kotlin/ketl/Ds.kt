package ketl

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

fun sqliteDatasource(
  dbPath: String = "./etl.db",
  driverClassName: String = "org.sqlite.JDBC"
): HikariDataSource {
  val config = HikariConfig().apply {
    jdbcUrl = "jdbc:sqlite:$dbPath"
    setDriverClassName(driverClassName)
    maximumPoolSize = 1
    transactionIsolation = "TRANSACTION_SERIALIZABLE"
    connectionTestQuery = "SELECT 1"
    addDataSourceProperty("cachePrepStmts", "true")
    addDataSourceProperty("prepStmtCacheSize", "250")
    addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
  }
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
  val config = HikariConfig().apply {
    jdbcUrl = url
    setDriverClassName(driverClassName)
    setUsername(username)
    setPassword(password)
    setMaximumPoolSize(maximumPoolSize)
    idleTimeout = idleTimeoutMillis
    maxLifetime = maxLifetimeMillis
    connectionTimeout = connectionTimeoutMillis
    connectionTestQuery = "SELECT 1"
    addDataSourceProperty("cachePrepStmts", "true")
    addDataSourceProperty("prepStmtCacheSize", "250")
    addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
  }
  return HikariDataSource(config)
}

