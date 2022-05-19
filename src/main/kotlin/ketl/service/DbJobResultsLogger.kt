package ketl.service

import ketl.adapter.pg.PgJobResultsRepo
import ketl.adapter.sqlite.SQLiteJobResultsRepo
import ketl.domain.DbDialect
import ketl.domain.JobResults
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
fun CoroutineScope.dbJobResultsLogger(
  dbDialect: DbDialect,
  ds: DataSource,
  schema: String? = "ketl",
  logMessages: LogMessages,
  minLogLevel: LogLevel,
  jobResults: JobResults,
  durationToKeep: Duration = 5.days,
  runCleanupEvery: Duration = 30.minutes,
  showSQL: Boolean = false,
) = launch {
  val log = NamedLog(
    name = "dbJobResultsLogger",
    minLogLevel = minLogLevel,
    logMessages = logMessages,
  )

  val repo = when (dbDialect) {
    DbDialect.PostgreSQL -> PgJobResultsRepo(
      schema = schema ?: "public",
      ds = ds,
    )
    DbDialect.SQLite -> SQLiteJobResultsRepo(ds = ds)
  }

  val createTableResult = repo.createTables()
  if (showSQL) {
    log.debug(createTableResult.toString())
  }
  if (createTableResult is SQLResult.Error) {
    throw createTableResult.error
  }

  var lastCleanup = LocalDateTime.now()

  val deleteBeforeResult = repo.deleteBefore(LocalDateTime.now() - durationToKeep.toJavaDuration())
  if (showSQL) {
    log.debug(deleteBeforeResult.toString())
  }
  if (deleteBeforeResult is SQLResult.Error) {
    throw deleteBeforeResult.error
  }

  jobResults.stream.collect { jobResult ->
    val timeSinceLastCleanup = lastCleanup.until(LocalDateTime.now(), ChronoUnit.SECONDS)

    if (timeSinceLastCleanup > runCleanupEvery.inWholeSeconds) {
      val result = repo.deleteBefore(LocalDateTime.now() - durationToKeep.toJavaDuration())
      if (showSQL) {
        log.debug(result.toString())
      }
      if (result is SQLResult.Error) {
        throw result.error
      }

      lastCleanup = LocalDateTime.now()
    }

    val addResult = repo.add(jobResult)
    if (showSQL) {
      log.debug(addResult.toString())
    }
    if (addResult is SQLResult.Error) {
      throw addResult.error
    }
  }
}
