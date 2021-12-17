package ketl.domain

import java.time.LocalDateTime

interface DbJobResultsRepo {
  suspend fun add(jobResult: JobResult)

  suspend fun createTables()

  suspend fun deleteBefore(ts: LocalDateTime)

  suspend fun getLatestResults(): Set<JobResult>
}
