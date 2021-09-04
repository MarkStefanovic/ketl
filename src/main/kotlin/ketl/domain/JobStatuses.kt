package ketl.domain

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.ExperimentalTime

data class Snapshot(val values: Set<JobStatus>)

@ExperimentalTime
class JobStatuses(
  private val jobs: List<Job<*>>,
  private val scope: CoroutineScope,
  private val dispatcher: CoroutineDispatcher = Dispatchers.Default,
) {
  private val current = ConcurrentHashMap<String, JobStatus>()

  private val _stream =
    MutableSharedFlow<JobStatus>(
      replay = 1,
      extraBufferCapacity = 100,
      onBufferOverflow = BufferOverflow.SUSPEND,
    )

  val stream = _stream.asSharedFlow()

  private val _snapshots = MutableStateFlow(Snapshot(emptySet()))

  val snapshots = _snapshots.asStateFlow()

  init {
    scope.launch(dispatcher) {
      jobs.forEach { job ->
        setStatus(JobStatus.Initial(job.name))
      }
    }
  }

  fun cancel(jobName: String) {
    setStatus(JobStatus.Cancelled(jobName))
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

  fun runningJobCount(): Int =
    current.values.count { it.statusName == JobStatusName.Running }

  private fun setStatus(status: JobStatus) {
    current[status.jobName] = status
    scope.launch(dispatcher) { _stream.emit(status) }
    _snapshots.value = Snapshot(current.values.toSet())
  }
}
