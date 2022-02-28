@file:Suppress("SqlResolve", "SqlInsertValues")

package ketl.adapter.sqlite

import ketl.domain.DbLogRepo
import ketl.domain.LogMessage
import ketl.domain.SQLResult
import java.sql.Timestamp
import java.time.LocalDateTime
import javax.sql.DataSource

class SQLiteLogRepo(private val ds: DataSource) : DbLogRepo {
  override fun createTable(): SQLResult {
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

    // language=SQLite
    val createTsIndexSQL = """
      |CREATE INDEX IF NOT EXISTS ix_log_ts 
      |  ON ketl_log (ts)
    """.trimMargin()

    // language=SQLite
    val createLogNameIndexSQL = """
      |CREATE INDEX IF NOT EXISTS ix_log_log_name 
      |  ON ketl_log (log_name)
    """.trimMargin()

    val sql = "$createTableSQL;\n$createTsIndexSQL;\n$createLogNameIndexSQL;"

    return try {
      ds.connection.use { con ->
        con.createStatement().use { statement ->
          statement.queryTimeout = 60

          statement.executeUpdate(createTableSQL)

          statement.executeUpdate(createTsIndexSQL)

          statement.executeUpdate(createLogNameIndexSQL)
        }
      }

      SQLResult.Success(sql = sql, parameters = null)
    } catch (e: Exception) {
      SQLResult.Error(sql = sql, error = e)
    }
  }

  override fun add(message: LogMessage): SQLResult {
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

    return try {
      ds.connection.use { con ->
        con.prepareStatement(sql).use { preparedStatement ->
          preparedStatement.queryTimeout = 60

          preparedStatement.setString(1, message.loggerName)
          preparedStatement.setString(2, message.level.name.lowercase())
          preparedStatement.setString(3, message.message)
          preparedStatement.setTimestamp(4, Timestamp.valueOf(message.ts))

          preparedStatement.executeUpdate()
        }

        SQLResult.Success(
          sql = sql,
          parameters = mapOf(
            "log_name" to message.loggerName,
            "log_level" to message.level.name.lowercase(),
            "message" to message.message,
            "ts" to Timestamp.valueOf(message.ts),
          ),
        )
      }
    } catch (e: Exception) {
      SQLResult.Error(sql = sql, error = e)
    }
  }

  override fun deleteBefore(ts: LocalDateTime): SQLResult {
    // language=SQLite
    val sql = """
      |DELETE FROM ketl_log 
      |WHERE ts < ?
    """.trimMargin()

    return try {
      ds.connection.use { con ->
        con.prepareStatement(sql).use { preparedStatement ->
          preparedStatement.queryTimeout = 60

          preparedStatement.setTimestamp(1, Timestamp.valueOf(ts))

          preparedStatement.executeUpdate()
        }
      }

      SQLResult.Success(sql = sql, parameters = mapOf("ts" to Timestamp.valueOf(ts)))
    } catch (e: Exception) {
      SQLResult.Error(sql = sql, error = e)
    }
  }
}
