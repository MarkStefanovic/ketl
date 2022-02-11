package ketl.domain

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.withContext
import java.util.concurrent.Executors

interface JobStatusState {
  suspend fun add(status: JobStatus)

  suspend fun currentStatusOf(jobName: String): JobStatus?

  suspend fun runningJobs(): Set<String>
}

interface JobStatuses {
  val stream: SharedFlow<JobStatus>

  val state: JobStatusState

  suspend fun add(jobStatus: JobStatus)
}

class DefaultJobStatusState : JobStatusState {
  private val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()

  private val statuses = mutableMapOf<String, JobStatus>()

  override suspend fun add(status: JobStatus) = withContext(dispatcher) {
    statuses[status.jobName] = status
  }

  override suspend fun currentStatusOf(jobName: String): JobStatus? = withContext(dispatcher) {
    statuses[jobName]
  }

  override suspend fun runningJobs(): Set<String> = withContext(dispatcher) {
    statuses.values.filter { it.isRunning }.map { it.jobName }.toSet()
  }
}

class DefaultJobStatuses : JobStatuses {
  private val _stream = MutableSharedFlow<JobStatus>(
    extraBufferCapacity = 100,
    onBufferOverflow = BufferOverflow.DROP_OLDEST,
  )

  override val stream = _stream.asSharedFlow()

  override suspend fun add(jobStatus: JobStatus) {
    _stream.emit(jobStatus)
    state.add(jobStatus)
  }

  override val state: JobStatusState = DefaultJobStatusState()
}
