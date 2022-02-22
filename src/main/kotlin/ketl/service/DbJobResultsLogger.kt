package ketl.service

import ketl.adapter.pg.PgJobResultsRepo
import ketl.adapter.sqlite.SQLiteJobResultsRepo
import ketl.domain.DbDialect
import ketl.domain.JobResults
import ketl.domain.LogLevel
import ketl.domain.LogMessages
import ketl.domain.NamedLog
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
fun CoroutineScope.dbJobResultsLogger(
  dbDialect: DbDialect,
  ds: DataSource,
  schema: String? = "ketl",
  logMessages: LogMessages,
  minLogLevel: LogLevel,
  jobResults: JobResults,
  durationToKeep: Duration = 5.days,
  runCleanupEvery: Duration = 30.minutes,
) = launch {
  val repo = when (dbDialect) {
    DbDialect.PostgreSQL -> PgJobResultsRepo(
      schema = schema ?: "public",
      ds = ds,
      log = NamedLog(
        name = "pgJobResultsRepo",
        logMessages = logMessages,
        minLogLevel = minLogLevel,
      ),
    )
    DbDialect.SQLite -> SQLiteJobResultsRepo(
      ds = ds,
      log = NamedLog(
        name = "sqliteJobResultsRepo",
        logMessages = logMessages,
        minLogLevel = minLogLevel,
      ),
    )
  }

  repo.createTables()

  var lastCleanup = LocalDateTime.now()

  repo.deleteBefore(LocalDateTime.now() - durationToKeep.toJavaDuration())

  jobResults.stream.collect { jobResult ->
    val timeSinceLastCleanup = lastCleanup.until(LocalDateTime.now(), ChronoUnit.SECONDS)

    if (timeSinceLastCleanup > runCleanupEvery.inWholeSeconds) {
      repo.deleteBefore(LocalDateTime.now() - durationToKeep.toJavaDuration())

      lastCleanup = LocalDateTime.now()
    }

    repo.add(jobResult)
  }
}
