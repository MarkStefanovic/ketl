package ketl.domain

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import java.time.LocalDateTime

interface Log {
  val stream: SharedFlow<LogMessage>

  suspend fun debug(name: String, message: String)

  suspend fun error(name: String, message: String)

  suspend fun info(name: String, message: String)

  suspend fun warning(name: String, message: String)
}

object DefaultLog : Log {
  private val _stream = MutableSharedFlow<LogMessage>(
    extraBufferCapacity = 100,
    onBufferOverflow = BufferOverflow.DROP_OLDEST,
  )
  override val stream = _stream.asSharedFlow()

  private suspend fun add(logMessage: LogMessage) {
    _stream.emit(logMessage)
  }

  override suspend fun debug(name: String, message: String) {
    add(LogMessage(loggerName = name, level = LogLevel.Debug, message = message, ts = LocalDateTime.now()))
  }

  override suspend fun error(name: String, message: String) {
    add(LogMessage(loggerName = name, level = LogLevel.Error, message = message, ts = LocalDateTime.now()))
  }

  override suspend fun info(name: String, message: String) {
    add(LogMessage(loggerName = name, level = LogLevel.Info, message = message, ts = LocalDateTime.now()))
  }

  override suspend fun warning(name: String, message: String) {
    add(LogMessage(loggerName = name, level = LogLevel.Warning, message = message, ts = LocalDateTime.now()))
  }
}
