package ketl.domain

import java.time.LocalDateTime

interface ResultRepo {
  fun add(result: JobResult)

  fun deleteBefore(ts: LocalDateTime)

  fun getLatestResultsForJob(jobName: String, n: Int): List<JobResult>
}
