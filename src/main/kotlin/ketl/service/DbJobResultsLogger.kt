package ketl.service

import ketl.adapter.pg.PgJobResultsRepo
import ketl.adapter.sqlite.SQLiteJobResultsRepo
import ketl.domain.DbJobResultsRepo
import ketl.domain.DefaultJobResults
import ketl.domain.JobResults
import kotlinx.coroutines.flow.collect
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import javax.sql.DataSource
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.toJavaDuration

@ExperimentalTime
suspend fun pgJobResultsLogger(
  ds: DataSource,
  schema: String = "ketl",
  jobResults: JobResults = DefaultJobResults,
  durationToKeep: Duration = Duration.days(5),
  runCleanupEvery: Duration = Duration.minutes(30),
) = dbJobResultsLogger(
  jobResults = jobResults,
  durationToKeep = durationToKeep,
  runCleanupEvery = runCleanupEvery,
  repo = PgJobResultsRepo(ds = ds, schema = schema),
)

@ExperimentalTime
suspend fun sqliteJobResultsLogger(
  ds: DataSource,
  jobResults: JobResults = DefaultJobResults,
  durationToKeep: Duration = Duration.days(5),
  runCleanupEvery: Duration = Duration.minutes(30),
) = dbJobResultsLogger(
  jobResults = jobResults,
  durationToKeep = durationToKeep,
  runCleanupEvery = runCleanupEvery,
  repo = SQLiteJobResultsRepo(ds = ds),
)

@ExperimentalTime
private suspend fun dbJobResultsLogger(
  jobResults: JobResults,
  durationToKeep: Duration,
  runCleanupEvery: Duration,
  repo: DbJobResultsRepo,
) {
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
