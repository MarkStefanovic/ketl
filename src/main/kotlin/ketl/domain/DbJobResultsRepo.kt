package ketl.domain

import java.time.LocalDateTime

interface DbJobResultsRepo {
  fun add(jobResult: JobResult): SQLResult

  fun createTables(): SQLResult

  fun deleteBefore(ts: LocalDateTime): SQLResult

  fun getLatestResults(): Pair<SQLResult, Set<JobResult>>
}
