@file:Suppress("unused")

package ketl

import ketl.domain.DbDialect
import ketl.domain.DefaultJobQueue
import ketl.domain.DefaultJobResults
import ketl.domain.DefaultJobStatuses
import ketl.domain.JobResults
import ketl.domain.JobService
import ketl.domain.Log
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
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import javax.sql.DataSource
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

fun CoroutineScope.heartbeat(
  log: Log,
  jobResults: JobResults,
  maxTimeToWait: Duration,
  timeBetweenChecks: Duration,
) = launch {
  while (isActive) {
    val latestResults = withTimeout(10.seconds) {
      jobResults.getLatestResults()
    }

    val latestEndTime = latestResults.maxOfOrNull { it.end }

    if (latestEndTime != null) {
      val secondsSinceLastResult = latestEndTime.until(LocalDateTime.now(), ChronoUnit.SECONDS)

      withTimeout(10.seconds) {
        log.info("The latest job results were received $secondsSinceLastResult seconds ago.")
      }

      if (secondsSinceLastResult > maxTimeToWait.inWholeSeconds) {
        log.info("Cancelling services...")
        cancel(
          CancellationException(
            "The latest job results were received $secondsSinceLastResult seconds ago.  " +
              "Something must have gone wrong."
          )
        )
      }
    }
    delay(timeBetweenChecks)
  }
}

@ExperimentalCoroutinesApi
@DelicateCoroutinesApi
@ExperimentalTime
fun start(
  jobService: JobService,
  logDs: DataSource?,
  logDialect: DbDialect?,
  logSchema: String?,
  maxSimultaneousJobs: Int = 10,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
  timeBetweenScans: Duration = 10.seconds,
  logJobMessagesToConsole: Boolean = false,
  logJobStatusChanges: Boolean = false,
  minLogLevel: LogLevel = LogLevel.Info,
) = runBlocking {
  if (logDs != null) {
    require(logDialect != null) {
      "If a logDs is provided, then logDialect is required."
    }
  }

  while (true) {
    val scope = CoroutineScope(Job() + Dispatchers.Default)

    try {
      val logMessages = LogMessages()

      val jobStatuses = DefaultJobStatuses()
      val jobResults = DefaultJobResults()
      val jobQueue = DefaultJobQueue()
      val log = NamedLog(
        name = "ketl",
        logMessages = logMessages,
        minLogLevel = minLogLevel,
      )

      scope.launch {
        if (logJobMessagesToConsole) {
          log.info("Launching consoleLogger...")
          consoleLogger(
            minLogLevel = minLogLevel,
            logMessages = logMessages.stream,
          )
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

        if (logDs != null) {
          log.info("Launching dbLogger...")
          dbLogger(
            dbDialect = logDialect!!,
            ds = logDs,
            schema = logSchema,
            minLogLevel = minLogLevel,
            logMessages = logMessages,
          )

          log.info("Launching dbJobResultsLogger...")
          dbJobResultsLogger(
            dbDialect = logDialect,
            ds = logDs,
            schema = logSchema,
            logMessages = logMessages,
            jobResults = jobResults,
            minLogLevel = minLogLevel,
          )
        }

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
        }

        log.info("Launching jobRunner...")
        jobRunner(
          queue = jobQueue,
          results = jobResults,
          statuses = jobStatuses,
          logMessages = logMessages,
          dispatcher = dispatcher,
          maxSimultaneousJobs = maxSimultaneousJobs,
          timeBetweenScans = timeBetweenScans,
          minLogLevel = minLogLevel,
        )
      }

      val job = scope.launch {
        log.info("Launching heartbeat...")
        heartbeat(
          log = NamedLog(
            name = "heartbeat",
            logMessages = logMessages,
            minLogLevel = minLogLevel,
          ),
          jobResults = jobResults,
          maxTimeToWait = 15.minutes,
          timeBetweenChecks = 5.minutes,
        )
      }

      job.join()
    } catch (e: Exception) {
      e.printStackTrace()
    } finally {
      scope.cancel()
    }

    println("Restarting in 5 minutes...")

    delay(5.minutes)
  }
}
