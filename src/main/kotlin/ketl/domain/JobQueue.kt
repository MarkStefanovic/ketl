package ketl.domain

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlin.time.ExperimentalTime

@ExperimentalTime
class JobQueue {
  private val _stream =
    MutableSharedFlow<ETLJob<*>>(
      replay = 0,
      extraBufferCapacity = 1000,
      onBufferOverflow = BufferOverflow.SUSPEND,
    )

  val stream = _stream.asSharedFlow()

  suspend fun add(job: ETLJob<*>) {
    _stream.emit(job)
  }
}
