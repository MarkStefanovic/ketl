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
import ketl.service.heartbeat
import ketl.service.jobStatusCleaner
import ketl.service.jobStatusLogger
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import javax.sql.DataSource
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

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
  minTimeBetweenRestarts: Duration = 5.minutes,
  checkForHeartbeatEvery: Duration = 5.minutes,
  considerDeadIfNoHeartbeatFor: Duration = 15.minutes,
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

          yield()
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
          dispatcher = dispatcher,
          maxSimultaneousJobs = maxSimultaneousJobs,
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
          maxTimeToWait = considerDeadIfNoHeartbeatFor,
          timeBetweenChecks = checkForHeartbeatEvery,
        )
      }

      job.join()
    } catch (e: Exception) {
      e.printStackTrace()
    } finally {
      scope.cancel()
    }

    println("Restarting in ${minTimeBetweenRestarts.inWholeSeconds} seconds...")

    delay(minTimeBetweenRestarts)
  }
}
