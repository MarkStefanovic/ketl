@file:Suppress("SqlResolve")

package ketl.adapter.sqlite

import ketl.domain.DbLogRepo
import ketl.domain.Log
import ketl.domain.LogMessage
import java.sql.Timestamp
import java.time.LocalDateTime
import javax.sql.DataSource

class SQLiteLogRepo(
  private val ds: DataSource,
  private val log: Log,
) : DbLogRepo {

  override suspend fun createTable() {
    ds.connection.use { con ->
      // language=SQLite
      val createTableSQL = """
        |CREATE TABLE IF NOT EXISTS ketl_log (
        |  id INTEGER PRIMARY KEY 
        |, log_name TEXT NOT NULL CHECK (LENGTH(log_name) > 0)
        |, log_level TEXT NOT NULL CHECK (log_level IN ('debug', 'error', 'info', 'warning'))
        |, message TEXT NOT NULL CHECK (LENGTH(message) > 0)
        |, ts DATETIME NOT NULL DEFAULT current_timestamp 
        |)
      """.trimMargin()

      log.debug(createTableSQL)

      // language=SQLite
      val createTsIndexSQL = """
        |CREATE INDEX IF NOT EXISTS ix_log_ts 
        |  ON ketl_log (ts)
      """.trimMargin()

      log.debug(createTsIndexSQL)

      // language=SQLite
      val createLogNameIndexSQL = """
        |CREATE INDEX IF NOT EXISTS ix_log_log_name 
        |  ON ketl_log (log_name)
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
    // language=SQLite
    val sql = """
      |INSERT INTO ketl_log (
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
    // language=SQLite
    val sql = """
      |DELETE FROM ketl_log 
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
