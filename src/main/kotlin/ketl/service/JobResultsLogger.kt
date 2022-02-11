package ketl.service

import ketl.domain.JobResults
import ketl.domain.Log

suspend fun jobResultsLogger(
  jobResults: JobResults,
  log: Log,
) {
  jobResults.stream.collect { jobResult ->
    log.info("Job result received: $jobResult")
  }
}
