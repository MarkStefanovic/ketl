package main.kotlin.domain

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.time.ExperimentalTime

data class Snapshot(val values: Set<JobStatus>)

@ExperimentalTime
class JobStatuses(
  private val scope: CoroutineScope,
  jobs: List<Job<*>>,
) {
  private val current: MutableMap<String, JobStatus> = mutableMapOf()

  private val lock = ReentrantLock()

  private val _stream =
    MutableSharedFlow<JobStatus>(
      replay = 1,
      extraBufferCapacity = 10,
      onBufferOverflow = BufferOverflow.SUSPEND,
    )

  val stream = _stream.asSharedFlow()

  private val _snapshots = MutableStateFlow<Snapshot>(Snapshot(emptySet()))

  val snapshots = _snapshots.asStateFlow()

  init {
    jobs.forEach { job -> setStatus(JobStatus.Initial(job.name)) }
  }

  fun running(jobName: String) {
    setStatus(JobStatus.Running(jobName))
  }

  fun failure(jobName: String, errorMessage: String) {
    setStatus(JobStatus.Failure(jobName = jobName, errorMessage = errorMessage))
  }

  fun success(jobName: String) {
    setStatus(JobStatus.Success(jobName))
  }

  fun getJobStatuses(): Map<String, JobStatus> = lock.withLock { current }

  fun getStatusForJob(jobName: String): JobStatus? = lock.withLock { current[jobName] }

  private fun setStatus(status: JobStatus) {
    lock.withLock {
      current[status.jobName] = status
      scope.launch { _stream.emit(status) }
      _snapshots.value = Snapshot(current.values.toSet())
    }
  }
}
