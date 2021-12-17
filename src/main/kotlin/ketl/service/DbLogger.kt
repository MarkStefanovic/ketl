package ketl.service

import ketl.adapter.SQLiteLogRepo
import ketl.domain.DbLogRepo
import ketl.domain.LogLevel
import ketl.domain.LogMessage
import ketl.domain.LogMessages
import ketl.domain.gte
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import javax.sql.DataSource
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.toJavaDuration

@ExperimentalTime
suspend fun dbLogger(
  ds: DataSource,
  logMessages: SharedFlow<LogMessage> = LogMessages.stream,
  minLogLevel: LogLevel = LogLevel.Info,
  durationToKeep: Duration = Duration.days(5),
  runCleanupEvery: Duration = Duration.minutes(30),
  repo: DbLogRepo = SQLiteLogRepo(ds = ds),
) {
  var lastCleanup = LocalDateTime.now()

  repo.createTable()

  repo.deleteBefore(LocalDateTime.now() - durationToKeep.toJavaDuration())

  logMessages.collect { logMessage ->
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
