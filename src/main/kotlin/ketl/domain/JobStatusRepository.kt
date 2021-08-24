package main.kotlin.ketl.domain

interface JobStatusRepository {
  fun upsert(status: JobStatus)
}
