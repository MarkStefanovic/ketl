package ketl.service

import ketl.domain.DefaultJobQueue
import ketl.domain.JobQueue
import ketl.domain.Log
import ketl.domain.NamedLog
import kotlinx.coroutines.flow.collect
import kotlin.time.ExperimentalTime

@ExperimentalTime
suspend fun jobQueueLogger(
  jobQueue: JobQueue = DefaultJobQueue,
  log: Log = NamedLog("jobQueueLogger"),
) {
  jobQueue.stream.collect { jobs ->
    val jobNamesCSV = jobs.joinToString(", ") { it.name }
    log.info(
      message = "Job Queue Status: $jobNamesCSV",
    )
  }
}
