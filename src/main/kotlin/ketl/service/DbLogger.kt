package ketl.service

import ketl.adapter.pg.PgLogRepo
import ketl.adapter.sqlite.SQLiteLogRepo
import ketl.domain.DbDialect
import ketl.domain.LogLevel
import ketl.domain.LogMessages
import ketl.domain.SQLResult
import ketl.domain.gte
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import javax.sql.DataSource
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.minutes
import kotlin.time.ExperimentalTime
import kotlin.time.toJavaDuration

@ExperimentalTime
fun CoroutineScope.dbLogger(
  ds: DataSource,
  dbDialect: DbDialect,
  logMessages: LogMessages,
  schema: String? = "ketl",
  minLogLevel: LogLevel = LogLevel.Info,
  durationToKeep: Duration = 5.days,
  runCleanupEvery: Duration = 30.minutes,
  excludeLogNames: Set<String> = setOf("jobStatusLogger", "ketl"),
  showSQL: Boolean = false,
) = launch {
  var lastCleanup = LocalDateTime.now()

  val repo = when (dbDialect) {
    DbDialect.PostgreSQL -> PgLogRepo(ds = ds, schema = schema ?: "public")
    DbDialect.SQLite -> SQLiteLogRepo(ds = ds)
  }

  when (val result = repo.createTable()) {
    is SQLResult.Error -> throw result.error
    is SQLResult.Success -> if (showSQL) println(result)
  }

  when (val result = repo.deleteBefore(LocalDateTime.now() - durationToKeep.toJavaDuration())) {
    is SQLResult.Error -> throw result.error
    is SQLResult.Success -> if (showSQL) println(result)
  }

  logMessages.stream.collect { logMessage ->
    if (logMessage.loggerName !in excludeLogNames) {
      if (logMessage.level gte minLogLevel) {
        val timeSinceLastCleanup = lastCleanup.until(LocalDateTime.now(), ChronoUnit.SECONDS)

        if (timeSinceLastCleanup > runCleanupEvery.inWholeSeconds) {
          when (val result = repo.deleteBefore(LocalDateTime.now() - durationToKeep.toJavaDuration())) {
            is SQLResult.Error -> throw result.error
            is SQLResult.Success -> if (showSQL) println(result)
          }

          lastCleanup = LocalDateTime.now()
        }

        when (val result = repo.add(logMessage)) {
          is SQLResult.Error -> throw result.error
          is SQLResult.Success -> if (showSQL) println(result)
        }
      }
    }
  }
}
