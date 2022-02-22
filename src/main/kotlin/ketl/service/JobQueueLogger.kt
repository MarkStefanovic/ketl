package ketl.service

import ketl.domain.JobQueue
import ketl.domain.Log
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlin.time.ExperimentalTime

@ExperimentalTime
fun CoroutineScope.jobQueueLogger(
  jobQueue: JobQueue,
  log: Log,
) = launch {
  jobQueue.stream.collect { jobs ->
    val jobNamesCSV = jobs.joinToString(", ") { it.name }
    log.info(
      message = "Job Queue Status: $jobNamesCSV",
    )
  }
}
