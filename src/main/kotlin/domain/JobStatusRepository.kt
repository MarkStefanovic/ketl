package main.kotlin.domain

interface JobStatusRepository {
  fun upsert(status: JobStatus)
}
