package ketl.domain

interface JobStatusRepository {
  fun upsert(status: JobStatus)
}
