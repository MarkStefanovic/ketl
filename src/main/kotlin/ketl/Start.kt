@file:Suppress("unused")

package ketl

import ketl.domain.DefaultJobQueue
import ketl.domain.DefaultJobResults
import ketl.domain.DefaultJobStatuses
import ketl.domain.JobQueue
import ketl.domain.JobResults
import ketl.domain.JobService
import ketl.domain.JobStatuses
import ketl.domain.KETLErrror
import ketl.domain.Log
import ketl.domain.LogLevel
import ketl.domain.LogMessages
import ketl.domain.NamedLog
import ketl.domain.jobRunner
import ketl.domain.jobScheduler
import ketl.service.consoleLogger
import ketl.service.jobStatusCleaner
import ketl.service.jobStatusLogger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

@ExperimentalCoroutinesApi
@DelicateCoroutinesApi
@ExperimentalTime
suspend fun run(
  jobService: JobService,
  logMessages: LogMessages,
  log: Log,
  jobQueue: JobQueue,
  jobStatuses: JobStatuses,
  jobResults: JobResults,
  maxSimultaneousJobs: Int = 10,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
  timeBetweenScans: Duration = 10.seconds,
) = coroutineScope {
  try {
    log.info("Starting services...")

    launch(dispatcher) {
      jobScheduler(
        log = NamedLog(name = "jobScheduler", logMessages = logMessages),
        jobService = jobService,
        queue = jobQueue,
        results = jobResults,
        statuses = jobStatuses,
        timeBetweenScans = timeBetweenScans,
      )
    }

    launch(dispatcher) {
      jobStatusCleaner(
        log = NamedLog(name = "jobStatusCleaner", logMessages = logMessages),
        jobService = jobService,
        jobStatuses = jobStatuses,
        timeBetweenScans = timeBetweenScans,
      )
    }

    launch(dispatcher) {
      jobRunner(
        queue = jobQueue,
        results = jobResults,
        statuses = jobStatuses,
        logMessages = logMessages,
        dispatcher = dispatcher,
        maxSimultaneousJobs = maxSimultaneousJobs,
        timeBetweenScans = timeBetweenScans,
      )
    }
  } catch (e: Throwable) {
    log.info(e.stackTraceToString())
    throw e
  }
}

suspend fun startHeartbeat(
  log: Log,
  jobResults: JobResults,
  maxTimeToWait: Duration,
  timeBetweenChecks: Duration,
) {
  while (true) {
    val latestResults = withTimeout(10.seconds) {
      jobResults.getLatestResults()
    }

    val latestEndTime = latestResults.maxOfOrNull { it.end }

    if (latestEndTime != null) {
      val secondsSinceLastResult = latestEndTime.until(LocalDateTime.now(), ChronoUnit.SECONDS)

      log.info("The latest job results were received $secondsSinceLastResult seconds ago.")

      if (secondsSinceLastResult > maxTimeToWait.inWholeSeconds) {
        throw KETLErrror.JobResultsStopped(secondsSinceLastResult.seconds)
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
  maxSimultaneousJobs: Int = 10,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
  timeBetweenScans: Duration = 10.seconds,
  restartOnFailure: Boolean = true,
  timeBetweenRestarts: Duration = 10.minutes,
  logJobMessagesToConsole: Boolean = false,
  logJobStatusChanges: Boolean = false,
) = runBlocking {
  while (true) {
    val logMessages = LogMessages()
    val jobStatuses = DefaultJobStatuses()

    if (logJobStatusChanges) {
      launch {
        jobStatusLogger(
          jobStatuses = jobStatuses,
          log = NamedLog(name = "jobStatusLogger", logMessages = logMessages),
        )
      }
    }

    if (logJobMessagesToConsole) {
      launch {
        consoleLogger(
          minLogLevel = LogLevel.Debug,
          logMessages = logMessages.stream,
        )
      }
    }

    val log = NamedLog(name = "ketl", logMessages = logMessages)
    val jobResults = DefaultJobResults()

    try {
      val job = run(
        jobService = jobService,
        logMessages = logMessages,
        log = log,
        jobQueue = DefaultJobQueue(),
        jobStatuses = jobStatuses,
        jobResults = jobResults,
        maxSimultaneousJobs = maxSimultaneousJobs,
        dispatcher = dispatcher,
        timeBetweenScans = timeBetweenScans,
      )

      job.invokeOnCompletion {
        try {
          job.cancelChildren(it as? CancellationException)
          launch(dispatcher) {
            log.info(message = "Children cancelled.")
          }
        } catch (e: Throwable) {
          launch(dispatcher) {
            log.info("An exception occurred while closing child coroutines: ${e.stackTraceToString()}")
          }
          throw e
        }
      }

      startHeartbeat(
        log = NamedLog(name = "heartbeat", logMessages = logMessages),
        jobResults = jobResults,
        maxTimeToWait = 15.minutes,
        timeBetweenChecks = 5.minutes,
      )

      job.join()
    } catch (e: Throwable) {
      e.printStackTrace()

      if (restartOnFailure) {
        log.info("Restarting in $timeBetweenRestarts...")

        delay(timeBetweenRestarts)
      } else {
        throw e
      }
    }
  }
}
