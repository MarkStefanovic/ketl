package ketl.service

import ketl.domain.JobResults
import ketl.domain.Log
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch

fun CoroutineScope.jobResultsLogger(
  jobResults: JobResults,
  log: Log,
) = launch {
  jobResults.stream.collect { jobResult ->
    log.info("Job result received: $jobResult")
  }
}
