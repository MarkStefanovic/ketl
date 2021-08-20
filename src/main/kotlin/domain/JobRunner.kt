package main.kotlin.domain

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.withTimeout
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.time.ExperimentalTime

@ExperimentalTime
class JobRunner(
  scope: CoroutineScope,
  private val log: LogMessages,
  private val queue: SharedFlow<Job<*>>,
  private val status: JobStatuses,
  private val results: JobResults,
  val maxSimultaneousJobs: Int,
) {
  private val _runningJobs = ConcurrentLinkedQueue<String>()

  init {
    val coroutineContext = newFixedThreadPoolContext(maxSimultaneousJobs, "capped_jobs")

    scope.launch {
      queue.collect { job ->
        if (!_runningJobs.contains(job.name)) {
          status.running(job.name)

          _runningJobs.add(job.name)

          launch(context = coroutineContext) {
            log.info("Starting ${job.name}...")

            val start = LocalDateTime.now()

            val result: JobResult =
              try {
                withTimeout(job.timeout.inWholeMilliseconds) {
                  runWithRetry(
                    job = job,
                    start = start,
                    retries = 0,
                  )
                }
              } catch (_: TimeoutCancellationException) {
                JobResult.Failure(
                  jobName = job.name,
                  start = start,
                  end = LocalDateTime.now(),
                  errorMessage = "Job timed out after ${job.timeout.inWholeSeconds} seconds.",
                )
              } catch (e: Exception) {
                JobResult.Failure(
                  jobName = job.name,
                  start = start,
                  end = LocalDateTime.now(),
                  errorMessage = e.stackTraceToString(),
                )
              }

            val msg =
              when (result) {
                is JobResult.Failure -> "${result.jobName} failed: ${result.errorMessage}"
                is JobResult.Success -> "${result.jobName} finished successfully."
              }
            log.info(msg)

            results.add(result)

            when (result) {
              is JobResult.Failure ->
                status.failure(jobName = job.name, errorMessage = result.errorMessage)
              is JobResult.Success -> status.success(jobName = job.name)
            }
          }
          _runningJobs.remove(job.name)
        }
      }
    }
  }
}

@ExperimentalTime
suspend fun runWithRetry(
  job: Job<*>,
  start: LocalDateTime,
  retries: Int,
): JobResult =
  try {
    job.run()
    JobResult.Success(
      jobName = job.name,
      start = start,
      end = LocalDateTime.now(),
    )
  } catch (e: Exception) {
    if (retries >= job.retries) {
      JobResult.Failure(
        jobName = job.name,
        start = start,
        end = LocalDateTime.now(),
        errorMessage = e.stackTraceToString(),
      )
    } else {
      runWithRetry(
        job = job,
        start = start,
        retries = retries + 1,
      )
    }
  }
