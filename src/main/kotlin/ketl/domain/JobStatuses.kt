package ketl.domain

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import java.util.concurrent.ConcurrentHashMap

interface JobStatuses {
  val stream: SharedFlow<Map<String, JobStatus>>

  suspend fun add(status: JobStatus)

  val runningJobCount: Int
}

object DefaultJobStatuses : JobStatuses {
  private val _stream = MutableSharedFlow<Map<String, JobStatus>>(
    extraBufferCapacity = 100,
    onBufferOverflow = BufferOverflow.DROP_OLDEST,
  )
  override val stream = _stream.asSharedFlow()

  private val statuses = ConcurrentHashMap<String, JobStatus>(emptyMap())

  override suspend fun add(status: JobStatus) {
    statuses[status.jobName] = status
    _stream.emit(statuses.toMap())
  }

  override val runningJobCount: Int
    get() = statuses.values.count { it is JobStatus.Running }
}
