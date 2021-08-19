package main.kotlin.domain

sealed class JobStatus(val jobName: String, val statusName: JobStatusName) {
  class Initial(jobName: String) :
    JobStatus(jobName = jobName, statusName = JobStatusName.Initial)

  class Running(jobName: String) :
    JobStatus(jobName = jobName, statusName = JobStatusName.Running)

  class Success(jobName: String) :
    JobStatus(jobName = jobName, statusName = JobStatusName.Successful)

  class Failure(jobName: String, val errorMessage: String) :
    JobStatus(jobName = jobName, statusName = JobStatusName.Failed)
}
