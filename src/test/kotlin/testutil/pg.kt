package testutil

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import getConfig
import javax.sql.DataSource

fun pgDataSource(): DataSource {
  val config = getConfig()

  val hikariConfig = HikariConfig().apply {
    jdbcUrl = config.pgURL
    username = config.pgUsername
    password = config.pgPassword
  }
  return HikariDataSource(hikariConfig)
}
