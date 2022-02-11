package ketl.service

import ketl.adapter.pg.PgLogRepo
import ketl.adapter.sqlite.SQLiteLogRepo
import ketl.domain.DbDialect
import ketl.domain.LogLevel
import ketl.domain.LogMessages
import ketl.domain.NamedLog
import ketl.domain.gte
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import javax.sql.DataSource
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.minutes
import kotlin.time.ExperimentalTime
import kotlin.time.toJavaDuration

@ExperimentalTime
suspend fun dbLogger(
  ds: DataSource,
  dbDialect: DbDialect,
  logMessages: LogMessages,
  schema: String? = "ketl",
  minLogLevel: LogLevel = LogLevel.Info,
  durationToKeep: Duration = 5.days,
  runCleanupEvery: Duration = 30.minutes,
  excludeLogNames: Set<String> = setOf("jobStatusLogger", "ketl"),
) {
  var lastCleanup = LocalDateTime.now()

  val repo = when (dbDialect) {
    DbDialect.PostgreSQL -> PgLogRepo(
      ds = ds,
      schema = schema ?: "public",
      log = NamedLog(
        name = "pgLogger",
        logMessages = logMessages,
        minLogLevel = minLogLevel,
      ),
    )
    DbDialect.SQLite -> SQLiteLogRepo(
      ds = ds,
      log = NamedLog(
        name = "sqliteLogger",
        logMessages = logMessages,
        minLogLevel = minLogLevel,
      ),
    )
  }

  repo.createTable()

  repo.deleteBefore(LocalDateTime.now() - durationToKeep.toJavaDuration())

  logMessages.stream.collect { logMessage ->
    if (logMessage.loggerName !in excludeLogNames) {
      if (logMessage.level gte minLogLevel) {
        val timeSinceLastCleanup = lastCleanup.until(LocalDateTime.now(), ChronoUnit.SECONDS)

        if (timeSinceLastCleanup > runCleanupEvery.inWholeSeconds) {
          repo.deleteBefore(LocalDateTime.now() - durationToKeep.toJavaDuration())

          lastCleanup = LocalDateTime.now()
        }

        repo.add(logMessage)
      }
    }
  }
}
