package ketl.service

import ketl.adapter.pg.PgJobStatusRepo
import ketl.adapter.sqlite.SQLiteJobStatusRepo
import ketl.domain.DbDialect
import ketl.domain.JobStatus
import ketl.domain.JobStatuses
import ketl.domain.LogLevel
import ketl.domain.LogMessages
import ketl.domain.NamedLog
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import javax.sql.DataSource
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.minutes
import kotlin.time.ExperimentalTime
import kotlin.time.toJavaDuration

@ExperimentalTime
suspend fun dbJobStatusLogger(
  ds: DataSource,
  dbDialect: DbDialect,
  schema: String? = "ketl",
  jobStatuses: JobStatuses,
  logMessages: LogMessages,
  minLogLevel: LogLevel,
  durationToKeep: Duration = 5.days,
  runCleanupEvery: Duration = 30.minutes,
) {
  val repo = when (dbDialect) {
    DbDialect.PostgreSQL -> PgJobStatusRepo(
      schema = schema ?: "public",
      ds = ds,
      log = NamedLog(
        name = "pgJobStatusRepo",
        logMessages = logMessages,
        minLogLevel = minLogLevel,
      )
    )
    DbDialect.SQLite -> SQLiteJobStatusRepo(
      ds = ds,
      log = NamedLog(
        name = "sqliteJobStatusRepo",
        logMessages = logMessages,
        minLogLevel = minLogLevel,
      )
    )
  }

  repo.createTables()

  repo.cancelRunningJobs()

  var lastCleanup = LocalDateTime.now()

  repo.deleteBefore(LocalDateTime.now() - durationToKeep.toJavaDuration())

  jobStatuses.stream.collect { jobStatus: JobStatus ->
    val timeSinceLastCleanup = lastCleanup.until(LocalDateTime.now(), ChronoUnit.SECONDS)

    if (timeSinceLastCleanup > runCleanupEvery.inWholeSeconds) {
      repo.deleteBefore(LocalDateTime.now() - durationToKeep.toJavaDuration())

      lastCleanup = LocalDateTime.now()
    }

    repo.add(jobStatus)
  }
}
