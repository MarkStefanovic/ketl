@file:Suppress("SqlResolve")

package ketl.adapter.pg

import ketl.domain.DbLogRepo
import ketl.domain.LogMessage
import ketl.domain.SQLResult
import java.sql.Timestamp
import java.time.LocalDateTime
import javax.sql.DataSource

class PgLogRepo(
  private val ds: DataSource,
  private val schema: String,
) : DbLogRepo {
  override fun createTable(): SQLResult {
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

    // language=PostgreSQL
    val createTsIndexSQL = """
      |CREATE INDEX IF NOT EXISTS ix_log_ts 
      |  ON $schema.log (ts)
    """.trimMargin()

    // language=PostgreSQL
    val createLogNameIndexSQL = """
      |CREATE INDEX IF NOT EXISTS ix_log_log_name 
      |  ON $schema.log (log_name)
    """.trimMargin()

    return try {
      ds.connection.use { con ->
        con.createStatement().use { statement ->
          statement.queryTimeout = 60

          statement.executeUpdate(createTableSQL)

          statement.executeUpdate(createTsIndexSQL)

          statement.executeUpdate(createLogNameIndexSQL)
        }
      }

      SQLResult.Success(
        sql = "$createTableSQL;\n$createTsIndexSQL;\n$createLogNameIndexSQL;",
        parameters = null,
      )
    } catch (e: Exception) {
      SQLResult.Error(
        sql = "$createTableSQL;\n$createTsIndexSQL;\n$createLogNameIndexSQL;",
        parameters = null,
        error = e,
      )
    }
  }

  override fun add(message: LogMessage): SQLResult {
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
      }

      SQLResult.Success(
        sql = sql,
        parameters = mapOf(
          "log_name" to message.loggerName,
          "log_level" to message.level.name.lowercase(),
          "message" to message.message,
          "ts" to Timestamp.valueOf(message.ts),
        )
      )
    } catch (e: Exception) {
      SQLResult.Error(
        sql = sql,
        parameters = mapOf("message" to message),
        error = e,
      )
    }
  }

  override fun deleteBefore(ts: LocalDateTime): SQLResult {
    // language=PostgreSQL
    val sql = """
      |DELETE FROM $schema.log 
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

      SQLResult.Success(
        sql = sql,
        parameters = mapOf("ts" to Timestamp.valueOf(ts)),
      )
    } catch (e: Exception) {
      SQLResult.Error(
        sql = sql,
        parameters = mapOf("ts" to Timestamp.valueOf(ts)),
        error = e,
      )
    }
  }
}
