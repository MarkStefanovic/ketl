package ketl.service

import ketl.adapter.pg.PgLogRepo
import ketl.adapter.sqlite.SQLiteLogRepo
import ketl.domain.DbLogRepo
import ketl.domain.LogLevel
import ketl.domain.LogMessage
import ketl.domain.LogMessages
import ketl.domain.gte
import kotlinx.coroutines.flow.SharedFlow
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import javax.sql.DataSource
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.minutes
import kotlin.time.ExperimentalTime
import kotlin.time.toJavaDuration

@ExperimentalTime
suspend fun pgLogger(
  ds: DataSource,
  schema: String = "ketl",
  logMessages: SharedFlow<LogMessage> = LogMessages.stream,
  minLogLevel: LogLevel = LogLevel.Info,
  durationToKeep: Duration = 5.days,
  runCleanupEvery: Duration = 30.minutes,
) = dbLogger(
  logMessages = logMessages,
  minLogLevel = minLogLevel,
  durationToKeep = durationToKeep,
  runCleanupEvery = runCleanupEvery,
  repo = PgLogRepo(ds = ds, schema = schema),
)

@ExperimentalTime
suspend fun sqliteLogger(
  ds: DataSource,
  logMessages: SharedFlow<LogMessage> = LogMessages.stream,
  minLogLevel: LogLevel = LogLevel.Info,
  durationToKeep: Duration = 5.days,
  runCleanupEvery: Duration = 30.minutes,
) = dbLogger(
  logMessages = logMessages,
  minLogLevel = minLogLevel,
  durationToKeep = durationToKeep,
  runCleanupEvery = runCleanupEvery,
  repo = SQLiteLogRepo(ds = ds),
)

@ExperimentalTime
private suspend fun dbLogger(
  repo: DbLogRepo,
  logMessages: SharedFlow<LogMessage> = LogMessages.stream,
  minLogLevel: LogLevel = LogLevel.Info,
  durationToKeep: Duration = 5.days,
  runCleanupEvery: Duration = 30.minutes,
  excludeLogNames: Set<String> = setOf("jobStatusLogger", "ketl"),
) {
  var lastCleanup = LocalDateTime.now()

  repo.createTable()

  repo.deleteBefore(LocalDateTime.now() - durationToKeep.toJavaDuration())

  logMessages.collect { logMessage ->
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
