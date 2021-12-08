package ketl.domain

import java.time.LocalDateTime

interface JobResultsRepo {
  suspend fun add(result: JobResult)

  suspend fun deleteBefore(ts: LocalDateTime)
}
