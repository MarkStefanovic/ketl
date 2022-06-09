@file:Suppress("unused")

package ketl

import ketl.domain.DbDialect
import ketl.domain.DefaultJobQueue
import ketl.domain.DefaultJobResults
import ketl.domain.DefaultJobStatuses
import ketl.domain.JobService
import ketl.domain.LogLevel
import ketl.domain.LogMessages
import ketl.domain.NamedLog
import ketl.domain.jobRunner
import ketl.domain.jobScheduler
import ketl.service.consoleLogger
import ketl.service.dbJobResultsLogger
import ketl.service.dbJobStatusLogger
import ketl.service.dbLogger
import ketl.service.jobStatusCleaner
import ketl.service.jobStatusLogger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import javax.sql.DataSource
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

@ExperimentalCoroutinesApi
@DelicateCoroutinesApi
@ExperimentalTime
suspend fun start(
  jobService: JobService,
  logDs: DataSource?,
  logDialect: DbDialect?,
  logSchema: String?,
  maxSimultaneousJobs: Int = 10,
  timeBetweenScans: Duration = 10.seconds,
  logJobMessagesToConsole: Boolean = false,
  logJobStatusChanges: Boolean = false,
  minLogLevel: LogLevel = LogLevel.Info,
  showSQL: Boolean = false,
) = coroutineScope {
  if (logDs != null) {
    require(logDialect != null) {
      "If a logDs is provided, then logDialect is required."
    }
  }

  val scope = CoroutineScope(Job() + Dispatchers.Default)

  val logMessages = LogMessages()

  val log = NamedLog(
    name = "ketl",
    logMessages = logMessages,
    minLogLevel = minLogLevel,
  )

  val jobStatuses = DefaultJobStatuses()
  val jobResults = DefaultJobResults()
  val jobQueue = DefaultJobQueue()

  val services = scope.launch {
    if (logJobMessagesToConsole) {
      consoleLogger(
        minLogLevel = minLogLevel,
        logMessages = logMessages.stream,
      )
      log.info("Launched consoleLogger.")

      yield()
    }

    if ((logDs != null) && (logDialect != null)) {
      dbLogger(
        dbDialect = logDialect,
        ds = logDs,
        schema = logSchema,
        minLogLevel = minLogLevel,
        logMessages = logMessages,
        showSQL = showSQL,
      )
      log.info("Launched dbLogger.")

      log.info("Launching dbJobResultsLogger...")
      dbJobResultsLogger(
        dbDialect = logDialect,
        ds = logDs,
        schema = logSchema,
        logMessages = logMessages,
        jobResults = jobResults,
        minLogLevel = minLogLevel,
      )

      yield()
    }

    log.info("Launching jobScheduler...")
    jobScheduler(
      log = NamedLog(
        name = "jobScheduler",
        logMessages = logMessages,
        minLogLevel = minLogLevel,
      ),
      jobService = jobService,
      queue = jobQueue,
      results = jobResults,
      statuses = jobStatuses,
      timeBetweenScans = timeBetweenScans,
    )

    yield()

    log.info("Launching jobStatusCleaner...")
    jobStatusCleaner(
      log = NamedLog(
        name = "jobStatusCleaner",
        logMessages = logMessages,
        minLogLevel = minLogLevel,
      ),
      jobService = jobService,
      jobStatuses = jobStatuses,
      timeBetweenScans = timeBetweenScans,
    )

    yield()

    if (logJobStatusChanges) {
      log.info("Launching jobStatusLogger...")
      jobStatusLogger(
        jobStatuses = jobStatuses,
        log = NamedLog(
          name = "jobStatusLogger",
          logMessages = logMessages,
          minLogLevel = minLogLevel,
        ),
      )

      yield()
    }

    if (logDs != null) {
      log.info("Launching dbJobStatusLogger...")
      dbJobStatusLogger(
        ds = logDs,
        dbDialect = logDialect!!,
        schema = logSchema,
        jobStatuses = jobStatuses,
        logMessages = logMessages,
        minLogLevel = minLogLevel,
      )

      yield()
    }

    log.info("Launching jobRunner...")
    jobRunner(
      queue = jobQueue,
      results = jobResults,
      statuses = jobStatuses,
      logMessages = logMessages,
      maxSimultaneousJobs = maxSimultaneousJobs,
      minLogLevel = minLogLevel,
    )
  }

  services.join()
  scope.cancel()
}
