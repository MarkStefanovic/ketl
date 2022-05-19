package ketl.domain

import java.time.LocalDateTime

interface DbJobStatusRepo {
  fun add(jobStatus: JobStatus): SQLResult

  fun cancelRunningJobs(): SQLResult

  fun createTables(): SQLResult

  fun currentStatus(): Pair<SQLResult, Set<JobStatus>>

  fun deleteBefore(ts: LocalDateTime): SQLResult
}
