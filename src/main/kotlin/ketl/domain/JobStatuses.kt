package ketl.domain

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import java.util.concurrent.ConcurrentHashMap

interface JobStatusStream {
  val statuses: SharedFlow<JobStatus>

  suspend fun add(jobStatus: JobStatus)
}

interface JobStatusState {
  fun add(status: JobStatus)

  fun currentStatusOf(jobName: String): JobStatus?

  val runningJobs: Set<String>
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
  private val statuses = ConcurrentHashMap<String, JobStatus>(emptyMap())

  override fun add(status: JobStatus) {
    statuses[status.jobName] = status
  }

  override fun currentStatusOf(jobName: String): JobStatus? =
    statuses[jobName]

  override val runningJobs: Set<String>
    get() = statuses.keys
}

object DefaultJobStatuses : JobStatuses {
  override val stream: JobStatusStream = DefaultJobStatusStream

  override val state: JobStatusState = DefaultJobStatusState
}
