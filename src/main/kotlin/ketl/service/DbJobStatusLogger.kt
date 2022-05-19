package ketl.service

import ketl.adapter.pg.PgJobStatusRepo
import ketl.adapter.sqlite.SQLiteJobStatusRepo
import ketl.domain.DbDialect
import ketl.domain.JobStatus
import ketl.domain.JobStatuses
import ketl.domain.LogLevel
import ketl.domain.LogMessages
import ketl.domain.NamedLog
import ketl.domain.SQLResult
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
fun CoroutineScope.dbJobStatusLogger(
  ds: DataSource,
  dbDialect: DbDialect,
  schema: String? = "ketl",
  jobStatuses: JobStatuses,
  logMessages: LogMessages,
  minLogLevel: LogLevel,
  durationToKeep: Duration = 5.days,
  runCleanupEvery: Duration = 30.minutes,
  showSQL: Boolean = false,
) = launch {
  val log = NamedLog(
    name = "dbJobStatusLogger",
    minLogLevel = minLogLevel,
    logMessages = logMessages,
  )

  val repo = when (dbDialect) {
    DbDialect.PostgreSQL -> PgJobStatusRepo(schema = schema ?: "public", ds = ds)
    DbDialect.SQLite -> SQLiteJobStatusRepo(ds = ds)
  }

  val createTablesResult = repo.createTables()
  if (showSQL) {
    log.debug(createTablesResult.toString())
  }
  if (createTablesResult is SQLResult.Error) {
    throw createTablesResult.error
  }

  val cancelRunningJobsResult = repo.cancelRunningJobs()
  if (showSQL) {
    log.debug(cancelRunningJobsResult.toString())
  }
  if (cancelRunningJobsResult is SQLResult.Error) {
    throw cancelRunningJobsResult.error
  }

  var lastCleanup = LocalDateTime.now()

  val deleteBeforeResult = repo.deleteBefore(LocalDateTime.now() - durationToKeep.toJavaDuration())
  if (showSQL) {
    log.debug(deleteBeforeResult.toString())
  }
  if (deleteBeforeResult is SQLResult.Error) {
    throw deleteBeforeResult.error
  }

  jobStatuses.stream.collect { jobStatus: JobStatus ->
    val timeSinceLastCleanup = lastCleanup.until(LocalDateTime.now(), ChronoUnit.SECONDS)

    if (timeSinceLastCleanup > runCleanupEvery.inWholeSeconds) {
      val deleteResult = repo.deleteBefore(LocalDateTime.now() - durationToKeep.toJavaDuration())
      if (showSQL) {
        log.debug(deleteResult.toString())
      }
      if (deleteResult is SQLResult.Error) {
        throw deleteResult.error
      }

      lastCleanup = LocalDateTime.now()
    }

    repo.add(jobStatus)
  }
}
