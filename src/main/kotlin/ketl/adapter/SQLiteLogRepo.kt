package ketl.adapter

import ketl.domain.LogMessage
import ketl.domain.LogRepo
import java.sql.Connection
import java.sql.Timestamp
import java.time.LocalDateTime
import javax.sql.DataSource

class SQLiteLogRepo(private val ds: DataSource) : LogRepo {

  fun createLogTable(con: Connection) {
    val sql = """
      |CREATE TABLE IF NOT EXISTS log (
      |  id INTEGER PRIMARY KEY 
      |, log_name TEXT NOT NULL CHECK (LENGTH(log_name) > 0)
      |, log_level TEXT NOT NULL CHECK (log_level IN ('debug', 'error', 'info', 'warning'))
      |, message TEXT NOT NULL CHECK (LENGTH(message) > 0)
      |, ts DATETIME NOT NULL DEFAULT current_timestamp 
      |)
    """.trimMargin()

    con.createStatement().use { statement ->
      statement.executeUpdate(sql)
    }
  }

  override fun add(message: LogMessage) {
    val sql = """
      |INSERT INTO log (
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
    ds.connection.use { con ->
      con.prepareStatement(sql).use { preparedStatement ->
        preparedStatement.setString(1, message.loggerName)
        preparedStatement.setString(2, message.level.name.lowercase())
        preparedStatement.setString(3, message.message)
        preparedStatement.setTimestamp(4, Timestamp.valueOf(message.ts))

        preparedStatement.executeUpdate()
      }
    }
  }

  override fun deleteBefore(ts: LocalDateTime) {
    val sql = """
      |DELETE FROM log 
      |WHERE log.ts < ?
    """.trimMargin()
    ds.connection.use { con ->
      con.prepareStatement(sql).use { preparedStatement ->
        preparedStatement.setTimestamp(1, Timestamp.valueOf(ts))

        preparedStatement.executeUpdate()
      }
    }
  }
}
