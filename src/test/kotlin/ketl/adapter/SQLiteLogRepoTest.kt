@file:Suppress("SqlDialectInspection")

package ketl.adapter

import ketl.domain.LogLevel
import ketl.domain.LogMessage
import org.sqlite.SQLiteDataSource
import java.time.LocalDateTime
import javax.sql.DataSource
import kotlin.test.Test
import kotlin.test.assertEquals

private fun testDataSource() = SQLiteDataSource().apply {
  url = "jdbc:sqlite:file:test?mode=memory&cache=shared"
}

private fun getLogMessages(ds: DataSource): List<LogMessage> {
  val sql = """
    |SELECT log_name, log_level, message, ts
    |FROM log
    |ORDER BY ts
  """.trimMargin()

  val result = mutableListOf<LogMessage>()
  ds.connection.use { connection ->
    connection.createStatement().use { statement ->
      statement.executeQuery(sql).use { rs ->
        while (rs.next()) {
          val logLevel: LogLevel = when (val lvl = rs.getString("log_level")) {
            "debug" -> LogLevel.Debug
            "error" -> LogLevel.Error
            "info" -> LogLevel.Info
            "warning" -> LogLevel.Warning
            else -> throw Exception("Unrecognized log level: $lvl")
          }
          val msg = LogMessage(
            loggerName = rs.getString("log_name"),
            level = logLevel,
            message = rs.getString("message"),
            ts = rs.getTimestamp("ts").toLocalDateTime(),
          )
          result.add(msg)
        }
      }
    }
  }

  return result
}

class SQLiteLogRepoTest {
  @Test
  fun add_happy_path() {
    val ds = testDataSource()

    val repo = SQLiteLogRepo(ds = ds)

    ds.connection.use { connection ->
      repo.createTable()

      val msg = LogMessage(loggerName = "test_log", level = LogLevel.Info, message = "test message", ts = LocalDateTime.of(2010, 1, 2, 3, 4, 5))

      repo.add(msg)

      val messages = getLogMessages(ds = ds)

      assertEquals(expected = listOf(msg), actual = messages)
    }
  }

  @Test
  fun deleteBefore_happy_path() {
    val ds = testDataSource()

    val repo = SQLiteLogRepo(ds = ds)

    ds.connection.use { connection ->
      repo.createTable()

      val msg1 = LogMessage(loggerName = "test_log", level = LogLevel.Info, message = "test message", ts = LocalDateTime.of(2010, 1, 2, 3, 4, 5))
      val msg2 = LogMessage(loggerName = "test_log", level = LogLevel.Info, message = "test message", ts = LocalDateTime.of(2011, 1, 2, 3, 4, 5))

      repo.add(msg1)
      repo.add(msg2)

      repo.deleteBefore(LocalDateTime.of(2010, 2, 3, 4, 5, 6))

      val messages = getLogMessages(ds = ds)

      assertEquals(expected = listOf(msg2), actual = messages)
    }
  }
}
