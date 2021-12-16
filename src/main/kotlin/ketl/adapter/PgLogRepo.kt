package ketl.adapter

import ketl.domain.DbLogRepo
import ketl.domain.LogMessage
import java.sql.Timestamp
import java.time.LocalDateTime
import javax.sql.DataSource

class PgLogRepo(
  private val ds: DataSource,
  private val schema: String,
) : DbLogRepo {

  override fun createTable() {
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

      // language=PostgreSQL
      val createTsIndexSQL = "CREATE INDEX IF NOT EXISTS ix_log_ts ON $schema.log (ts)"

      // language=PostgreSQL
      val createLogNameIndexSQL = "CREATE INDEX IF NOT EXISTS ix_log_log_name ON $schema.log (log_name)"

      con.createStatement().use { statement ->
        statement.executeUpdate(createTableSQL)

        statement.executeUpdate(createTsIndexSQL)

        statement.executeUpdate(createLogNameIndexSQL)
      }
    }
  }

  override fun add(message: LogMessage) {
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
    // language=PostgreSQL
    val sql = """
      |DELETE FROM $schema.log 
      |WHERE ts < ?
    """.trimMargin()
    ds.connection.use { con ->
      con.prepareStatement(sql).use { preparedStatement ->
        preparedStatement.setTimestamp(1, Timestamp.valueOf(ts))

        preparedStatement.executeUpdate()
      }
    }
  }
}
