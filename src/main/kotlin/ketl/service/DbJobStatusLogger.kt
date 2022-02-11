package ketl.service

import ketl.adapter.pg.PgJobStatusRepo
import ketl.adapter.sqlite.SQLiteJobStatusRepo
import ketl.domain.DbJobStatusRepo
import ketl.domain.JobStatus
import ketl.domain.JobStatuses
import ketl.domain.Log
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import javax.sql.DataSource
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.minutes
import kotlin.time.ExperimentalTime
import kotlin.time.toJavaDuration

@ExperimentalTime
suspend fun pgJobStatusLogger(
  ds: DataSource,
  schema: String = "ketl",
  log: Log,
  jobStatuses: JobStatuses,
  durationToKeep: Duration = 5.days,
  runCleanupEvery: Duration = 30.minutes,
) = dbJobStatusLogger(
  jobStatuses = jobStatuses,
  durationToKeep = durationToKeep,
  runCleanupEvery = runCleanupEvery,
  repo = PgJobStatusRepo(schema = schema, ds = ds, log = log)
)

@ExperimentalTime
suspend fun sqliteJobStatusLogger(
  ds: DataSource,
  jobStatuses: JobStatuses,
  log: Log,
  durationToKeep: Duration = 5.days,
  runCleanupEvery: Duration = 30.minutes,
) = dbJobStatusLogger(
  jobStatuses = jobStatuses,
  durationToKeep = durationToKeep,
  runCleanupEvery = runCleanupEvery,
  repo = SQLiteJobStatusRepo(ds = ds, log = log)
)

@ExperimentalTime
private suspend fun dbJobStatusLogger(
  repo: DbJobStatusRepo,
  jobStatuses: JobStatuses,
  durationToKeep: Duration = 5.days,
  runCleanupEvery: Duration = 30.minutes,
) {
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
