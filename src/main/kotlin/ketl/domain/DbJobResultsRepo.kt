package ketl.domain

import java.time.LocalDateTime

interface DbJobResultsRepo {
  fun add(jobResult: JobResult)

  fun createTables()

  fun deleteBefore(ts: LocalDateTime)

  fun getLatestResults(): Set<JobResult>
}
