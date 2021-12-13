package ketl.domain

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.withContext
import java.util.concurrent.Executors

interface JobStatusStream {
  val statuses: SharedFlow<JobStatus>

  suspend fun add(jobStatus: JobStatus)
}

interface JobStatusState {
  suspend fun add(status: JobStatus)

  suspend fun currentStatusOf(jobName: String): JobStatus?

  suspend fun runningJobs(): Set<String>
}

interface JobStatuses {
  val stream: JobStatusStream

  val state: JobStatusState

  suspend fun add(jobStatus: JobStatus) {
    stream.add(jobStatus)
    state.add(jobStatus)
  }
}

object DefaultJobStatusStream : JobStatusStream {
  private val _stream = MutableSharedFlow<JobStatus>(
    extraBufferCapacity = 100,
    onBufferOverflow = BufferOverflow.DROP_OLDEST,
  )

  override val statuses = _stream.asSharedFlow()

  override suspend fun add(jobStatus: JobStatus) {
    _stream.emit(jobStatus)
  }
}

object DefaultJobStatusState : JobStatusState {
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

object DefaultJobStatuses : JobStatuses {
  override val stream: JobStatusStream = DefaultJobStatusStream

  override val state: JobStatusState = DefaultJobStatusState
}
