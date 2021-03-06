package ketl.domain

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import java.time.LocalDateTime
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.ExperimentalTime

@ExperimentalCoroutinesApi
@ExperimentalTime
@DelicateCoroutinesApi
fun CoroutineScope.jobRunner(
  queue: JobQueue,
  results: JobResults,
  statuses: JobStatuses,
  logMessages: LogMessages,
  maxSimultaneousJobs: Int,
  minLogLevel: LogLevel,
) = launch {
  val mutex = Mutex()

  val jobs = mutableMapOf<String, Job>()

  val log = NamedLog(name = "jobRunner", logMessages = logMessages, minLogLevel = minLogLevel)

  while (isActive) {
    val ketlJob = queue.pop()

    if (ketlJob == null) {
      log.debug("No job was found in the queue.")
    } else {
      log.debug("Starting ${ketlJob.name}...")

      val jobLog = NamedLog(name = ketlJob.name, logMessages = logMessages, minLogLevel = minLogLevel)

      mutex.withLock {
        val activeJobs = jobs.filter { it.value.isActive }.map { it.key }

//        println("Jobs active status: ${jobs.map { "${it.key}: ${it.value.isActive}" }}")

        if ((activeJobs.count() < maxSimultaneousJobs) && (ketlJob.name !in activeJobs)) {
          val job = launch {
            runJob(
              results = results,
              statuses = statuses,
              job = ketlJob,
              log = jobLog,
            )
          }

          jobs[ketlJob.name] = job
        }
      }
    }
  }
}

@DelicateCoroutinesApi
@ExperimentalTime
private suspend fun runJob(
  results: JobResults,
  statuses: JobStatuses,
  log: Log,
  job: KETLJob,
) {
  val start = LocalDateTime.now()

  try {
    withTimeout(timeout = job.timeout) {
      log.debug("Starting ${job.name}...")

      ensureActive()

      statuses.add(JobStatus.Running(jobName = job.name, ts = start))

      val result = runWithRetry(
        job = job,
        log = log,
        start = start,
        retries = 0,
      )
      results.add(result)

      ensureActive()

      when (result) {
        is JobResult.Cancelled -> {
          statuses.add(
            JobStatus.Cancelled(
              jobName = job.name,
              ts = LocalDateTime.now(),
            )
          )
          log.info("${result.jobName} was cancelled.")
        }
        is JobResult.Failed -> {
          statuses.add(
            JobStatus.Failed(
              jobName = job.name,
              ts = LocalDateTime.now(),
              errorMessage = result.errorMessage,
            )
          )
          log.error("${result.jobName} failed: ${result.errorMessage}")
        }
        is JobResult.Successful -> {
          statuses.add(
            JobStatus.Success(
              jobName = job.name,
              ts = LocalDateTime.now(),
            )
          )
          log.debug("${result.jobName} finished successfully.")
        }
        is JobResult.Skipped -> {
          statuses.add(
            JobStatus.Skipped(
              jobName = job.name,
              ts = LocalDateTime.now(),
              reason = result.reason,
            )
          )
          log.info("${result.jobName} was skipped.")
        }
      }

      ensureActive()

      log.debug("Finished ${job.name}")
    }
  } catch (e: Exception) {
    if (e is CancellationException) {
      if (e is TimeoutCancellationException) {
        log.error("${job.name} timed out.")
      } else {
        log.info("Cancelled ${job.name}.")
        throw e
      }
    }

    statuses.add(
      JobStatus.Failed(
        jobName = job.name,
        ts = LocalDateTime.now(),
        errorMessage = e.message ?: "No error message was provided.",
      )
    )

    results.add(
      JobResult.Failed(
        jobName = job.name,
        start = start,
        end = LocalDateTime.now(),
        errorMessage = e.message ?: "No error message was provided.",
      )
    )

    log.error("An unexpected error occurred while running ${job.name}: ${e.message}\n${e.stackTraceToString()}")
  }
}

@ExperimentalTime
suspend fun runWithRetry(
  job: KETLJob,
  log: Log,
  retries: Int,
  start: LocalDateTime,
): JobResult =
  try {
    when (val status: Status = job.run(log)) {
      is Status.Failure -> {
        if (retries >= job.maxRetries) {
          JobResult.Failed(
            jobName = job.name,
            start = start,
            end = LocalDateTime.now(),
            errorMessage = status.errorMessage,
          )
        } else {
          runWithRetry(
            job = job,
            log = log,
            start = start,
            retries = retries + 1,
          )
        }
      }
      is Status.Skipped -> JobResult.Skipped(
        jobName = job.name,
        start = start,
        end = LocalDateTime.now(),
        reason = status.reason,
      )
      Status.Success -> JobResult.Successful(
        jobName = job.name,
        start = start,
        end = LocalDateTime.now(),
      )
    }
  } catch (e: Exception) {
    if (e is CancellationException) {
      throw e
    }

    log.error("An exception occurred while running ${job.name}: ${e.message}\n${e.stackTraceToString()}")

    if (retries >= job.maxRetries) {
      JobResult.Failed(
        jobName = job.name,
        start = start,
        end = LocalDateTime.now(),
        errorMessage = e.stackTraceToString(),
      )
    } else {
      log.info("${job.name} running retry ${retries + 1} of ${job.maxRetries}...")

      runWithRetry(
        job = job,
        log = log,
        start = start,
        retries = retries + 1,
      )
    }
  }
