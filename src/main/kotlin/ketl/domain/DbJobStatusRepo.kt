package ketl.domain

import java.time.LocalDateTime

interface DbJobStatusRepo {
  suspend fun add(jobStatus: JobStatus)

  suspend fun createTables()

  suspend fun currentStatus(): Set<JobStatus>

  suspend fun deleteBefore(ts: LocalDateTime)
}
