package ketl.domain

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.ExperimentalTime

data class Snapshot(val values: Set<JobStatus>)

@ExperimentalTime
class JobStatuses(
  private val scope: CoroutineScope,
  jobs: List<Job<*>>,
) {
  private val current: MutableMap<String, JobStatus> = mutableMapOf()

  private val mutex = Mutex()

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
    scope.launch {
      jobs.forEach { job ->
        setStatus(JobStatus.Initial(job.name))
      }
    }
  }

  suspend fun cancel(jobName: String) {
    setStatus(JobStatus.Cancelled(jobName))
  }

  suspend fun running(jobName: String) {
    setStatus(JobStatus.Running(jobName))
  }

  suspend fun failure(jobName: String, errorMessage: String) {
    setStatus(JobStatus.Failure(jobName = jobName, errorMessage = errorMessage))
  }

  suspend fun success(jobName: String) {
    setStatus(JobStatus.Success(jobName))
  }

  suspend fun getJobStatuses(): Map<String, JobStatus> = mutex.withLock { current }

  suspend fun getRunningJobCount(): Int =
    mutex.withLock {
      current.values.count { it.statusName == JobStatusName.Running }
    }

  suspend fun getStatusForJob(jobName: String): JobStatus? =
    mutex.withLock {
      current[jobName]
    }

  private suspend fun setStatus(status: JobStatus) {
    mutex.withLock {
      current[status.jobName] = status
      scope.launch { _stream.emit(status) }
      _snapshots.value = Snapshot(current.values.toSet())
    }
  }
}
