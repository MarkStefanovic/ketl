package ketl.service

import ketl.adapter.pg.PgJobStatusRepo
import ketl.adapter.sqlite.SQLiteJobStatusRepo
import ketl.domain.DbJobStatusRepo
import ketl.domain.DefaultJobStatuses
import ketl.domain.JobStatus
import ketl.domain.JobStatuses
import kotlinx.coroutines.flow.collect
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import javax.sql.DataSource
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.toJavaDuration

@ExperimentalTime
suspend fun pgJobStatusLogger(
  ds: DataSource,
  schema: String = "ketl",
  jobStatuses: JobStatuses = DefaultJobStatuses,
  durationToKeep: Duration = Duration.days(5),
  runCleanupEvery: Duration = Duration.minutes(30),
) = dbJobStatusLogger(
  jobStatuses = jobStatuses,
  durationToKeep = durationToKeep,
  runCleanupEvery = runCleanupEvery,
  repo = PgJobStatusRepo(schema = schema, ds = ds)
)

@ExperimentalTime
suspend fun sqliteJobStatusLogger(
  ds: DataSource,
  jobStatuses: JobStatuses = DefaultJobStatuses,
  durationToKeep: Duration = Duration.days(5),
  runCleanupEvery: Duration = Duration.minutes(30),
) = dbJobStatusLogger(
  jobStatuses = jobStatuses,
  durationToKeep = durationToKeep,
  runCleanupEvery = runCleanupEvery,
  repo = SQLiteJobStatusRepo(ds = ds)
)

@ExperimentalTime
private suspend fun dbJobStatusLogger(
  repo: DbJobStatusRepo,
  jobStatuses: JobStatuses = DefaultJobStatuses,
  durationToKeep: Duration = Duration.days(5),
  runCleanupEvery: Duration = Duration.minutes(30),
) {
  var lastCleanup = LocalDateTime.now()

  repo.createTables()

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
