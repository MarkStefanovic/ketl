@file:Suppress("unused")

package ketl

import ketl.domain.DefaultJobQueue
import ketl.domain.DefaultJobResults
import ketl.domain.DefaultJobStatuses
import ketl.domain.JobQueue
import ketl.domain.JobResults
import ketl.domain.JobService
import ketl.domain.JobStatuses
import ketl.domain.Log
import ketl.domain.LogMessage
import ketl.domain.LogMessages
import ketl.domain.NamedLog
import ketl.domain.jobRunner
import ketl.domain.jobScheduler
import ketl.service.jobStatusCleaner
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

@ExperimentalCoroutinesApi
@DelicateCoroutinesApi
@ExperimentalTime
suspend fun run(
  jobService: JobService,
  logMessages: SharedFlow<LogMessage> = LogMessages.stream,
  log: Log = NamedLog(name = "ketl", stream = logMessages),
  jobQueue: JobQueue = DefaultJobQueue,
  jobStatuses: JobStatuses = DefaultJobStatuses,
  jobResults: JobResults = DefaultJobResults,
  maxSimultaneousJobs: Int = 10,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
  timeBetweenScans: Duration = 10.seconds,
) = supervisorScope {
  try {
    log.info("Starting services...")

    val job = launch(dispatcher) {
      jobScheduler(
        jobService = jobService,
        queue = jobQueue,
        results = jobResults,
        statuses = jobStatuses,
        timeBetweenScans = timeBetweenScans,
      )
    }

    launch(dispatcher) {
      jobStatusCleaner(
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

    job.invokeOnCompletion {
      it?.printStackTrace()

      try {
        dispatcher.cancelChildren(it as? CancellationException)
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

    job
  } catch (e: Throwable) {
    log.info(e.stackTraceToString())
    throw e
  }
}

@ExperimentalCoroutinesApi
@DelicateCoroutinesApi
@ExperimentalTime
fun start(
  jobService: JobService,
  logMessages: SharedFlow<LogMessage> = LogMessages.stream,
  log: Log = NamedLog(name = "ketl", stream = logMessages),
  jobQueue: JobQueue = DefaultJobQueue,
  jobStatuses: JobStatuses = DefaultJobStatuses,
  jobResults: JobResults = DefaultJobResults,
  maxSimultaneousJobs: Int = 10,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
  timeBetweenScans: Duration = 10.seconds,
  restartOnFailure: Boolean = true,
  timeBetweenRestarts: Duration = 10.minutes,
) = runBlocking {
  while (true) {
    try {
      run(
        jobService = jobService,
        logMessages = logMessages,
        log = log,
        jobQueue = jobQueue,
        jobStatuses = jobStatuses,
        jobResults = jobResults,
        maxSimultaneousJobs = maxSimultaneousJobs,
        dispatcher = dispatcher,
        timeBetweenScans = timeBetweenScans,
      )
    } catch (e: Throwable) {
      if (restartOnFailure) {
        println("Restarting in $timeBetweenRestarts...")

        delay(timeBetweenRestarts)
      } else {
        throw e
      }
    }
  }
}
