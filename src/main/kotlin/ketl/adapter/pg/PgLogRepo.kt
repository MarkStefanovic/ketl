package ketl.adapter.pg

import ketl.domain.DbLogRepo
import ketl.domain.Log
import ketl.domain.LogMessage
import ketl.domain.NamedLog
import java.sql.Timestamp
import java.time.LocalDateTime
import javax.sql.DataSource

class PgLogRepo(
  private val ds: DataSource,
  private val schema: String,
  private val log: Log = NamedLog("PgLogRepo"),
) : DbLogRepo {

  override suspend fun createTable() {
    ds.connection.use { con ->
      // language=PostgreSQL
      val createTableSQL = """
      |CREATE TABLE IF NOT EXISTS $schema.log (
      |  id SERIAL PRIMARY KEY
      |, log_name TEXT NOT NULL CHECK (LENGTH(log_name) > 0)
      |, log_level TEXT NOT NULL CHECK (log_level IN ('debug', 'error', 'info', 'warning'))
      |, message TEXT NOT NULL CHECK (LENGTH(message) > 0)
      |, ts TIMESTAMPTZ(0) NOT NULL DEFAULT current_timestamp 
      |)
    """.trimMargin()

      log.debug(createTableSQL)

      // language=PostgreSQL
      val createTsIndexSQL = """
        |-- noinspection SqlResolve @ table/"log"
        |CREATE INDEX IF NOT EXISTS ix_log_ts 
        |  ON $schema.log (ts)
      """.trimMargin()

      log.debug(createTsIndexSQL)

      // language=PostgreSQL
      val createLogNameIndexSQL = """
        |-- noinspection SqlResolve @ table/"log"
        |CREATE INDEX IF NOT EXISTS ix_log_log_name 
        |  ON $schema.log (log_name)
      """.trimMargin()

      log.debug(createLogNameIndexSQL)

      con.createStatement().use { statement ->
        statement.queryTimeout = 60

        statement.executeUpdate(createTableSQL)

        statement.executeUpdate(createTsIndexSQL)

        statement.executeUpdate(createLogNameIndexSQL)
      }
    }
  }

  override suspend fun add(message: LogMessage) {
    // language=PostgreSQL
    val sql = """
      |INSERT INTO $schema.log (
      |  log_name
      |, log_level
      |, message
      |, ts
      |) VALUES (
      |  ?
      |, ?
      |, ?
      |, ?
      |)
    """.trimMargin()

    log.debug(sql)

    ds.connection.use { con ->
      con.prepareStatement(sql).use { preparedStatement ->
        preparedStatement.queryTimeout = 60

        preparedStatement.setString(1, message.loggerName)
        preparedStatement.setString(2, message.level.name.lowercase())
        preparedStatement.setString(3, message.message)
        preparedStatement.setTimestamp(4, Timestamp.valueOf(message.ts))

        preparedStatement.executeUpdate()
      }
    }
  }

  override suspend fun deleteBefore(ts: LocalDateTime) {
    // language=PostgreSQL
    val sql = """
      |DELETE FROM $schema.log 
      |WHERE ts < ?
    """.trimMargin()

    log.debug(sql)

    ds.connection.use { con ->
      con.prepareStatement(sql).use { preparedStatement ->
        preparedStatement.queryTimeout = 60

        preparedStatement.setTimestamp(1, Timestamp.valueOf(ts))

        preparedStatement.executeUpdate()
      }
    }
  }
}
