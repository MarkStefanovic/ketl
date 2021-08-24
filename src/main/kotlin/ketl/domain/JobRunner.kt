package main.kotlin.ketl.domain

import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.withTimeout
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.ExperimentalTime

@ExperimentalTime
suspend fun jobRunner(
  log: LogMessages,
  queue: SharedFlow<Job<*>>,
  status: JobStatuses,
  results: JobResults,
  maxSimultaneousJobs: Int,
) = coroutineScope {
  val runningJobs = ConcurrentLinkedQueue<String>()

  val coroutineContext = newFixedThreadPoolContext(maxSimultaneousJobs, "capped_jobs")

  queue.collect { job ->
    if (!runningJobs.contains(job.name)) {
      status.running(job.name)

      runningJobs.add(job.name)

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
            if (e is CancellationException) {
              println("${job.name} cancelled")
              throw e
            } else {
              JobResult.Failure(
                jobName = job.name,
                start = start,
                end = LocalDateTime.now(),
                errorMessage = e.stackTraceToString(),
              )
            }
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
      runningJobs.remove(job.name)
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
