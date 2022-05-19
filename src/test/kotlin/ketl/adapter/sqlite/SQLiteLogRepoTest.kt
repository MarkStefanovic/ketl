@file:Suppress("SqlDialectInspection")

package ketl.adapter.sqlite

import ketl.domain.LogLevel
import ketl.domain.LogMessage
import kotlinx.coroutines.runBlocking
import org.sqlite.SQLiteDataSource
import java.sql.Connection
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals

private fun testDataSource() = SQLiteDataSource().apply {
  url = "jdbc:sqlite:file:test?mode=memory&cache=shared"
}

private fun getLogMessages(con: Connection): List<LogMessage> {
  val sql = """
    |SELECT log_name, log_level, message, ts
    |FROM ketl_log
    |ORDER BY ts
  """.trimMargin()

  val result = mutableListOf<LogMessage>()
  con.createStatement().use { statement ->
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

  return result
}

class SQLiteLogRepoTest {
  @Test
  fun add_happy_path() = runBlocking {
    val ds = testDataSource()

    ds.connection.use { con ->
      val repo = SQLiteLogRepo(ds = ds)

      repo.createTable()

      val msg = LogMessage(
        loggerName = "test_log",
        level = LogLevel.Info,
        message = "test message",
        ts = LocalDateTime.of(2010, 1, 2, 3, 4, 5),
      )

      repo.add(msg)

      val messages = getLogMessages(con = con)

      assertEquals(expected = listOf(msg), actual = messages)
    }
  }

  @Test
  fun deleteBefore_happy_path() = runBlocking {
    val ds = testDataSource()

    ds.connection.use { con ->
      val repo = SQLiteLogRepo(ds = ds)

      repo.createTable()

      val msg1 = LogMessage(
        loggerName = "test_log",
        level = LogLevel.Info,
        message = "test message",
        ts = LocalDateTime.of(2010, 1, 2, 3, 4, 5),
      )
      val msg2 = LogMessage(
        loggerName = "test_log",
        level = LogLevel.Info,
        message = "test message",
        ts = LocalDateTime.of(2011, 1, 2, 3, 4, 5),
      )

      repo.add(msg1)
      repo.add(msg2)

      repo.deleteBefore(LocalDateTime.of(2010, 2, 3, 4, 5, 6))

      val messages = getLogMessages(con = con)

      assertEquals(expected = listOf(msg2), actual = messages)
    }
  }
}
