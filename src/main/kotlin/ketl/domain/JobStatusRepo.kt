package ketl.domain

interface JobStatusRepo {
  fun upsert(status: JobStatus)
}
