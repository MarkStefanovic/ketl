package ketl.service

import ketl.adapter.SQLiteJobResultsRepo
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
suspend fun jobResultsDbLogger(
  ds: DataSource,
  jobResults: JobResults = DefaultJobResults,
  durationToKeep: Duration = Duration.days(5),
  runCleanupEvery: Duration = Duration.minutes(30),
  repo: DbJobResultsRepo = SQLiteJobResultsRepo(ds = ds),
) {
  var lastCleanup = LocalDateTime.now()

  repo.createTables()

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
