package ketl.domain

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import java.time.LocalDateTime

class LogMessages {
  private val _stream = MutableSharedFlow<LogMessage>(
    extraBufferCapacity = 100,
    onBufferOverflow = BufferOverflow.DROP_OLDEST,
  )

  val stream: SharedFlow<LogMessage> = _stream.asSharedFlow()

  suspend fun emit(logMessage: LogMessage) {
    _stream.emit(logMessage)
  }
}

interface Log {
  suspend fun debug(message: String)

  suspend fun error(message: String)

  suspend fun info(message: String)

  suspend fun warning(message: String)
}

data class NamedLog(
  val name: String,
  val minLogLevel: LogLevel,
  private val logMessages: LogMessages,
) : Log {

  override suspend fun debug(message: String) {
    add(LogMessage(loggerName = name, level = LogLevel.Debug, message = message, ts = LocalDateTime.now()))
  }

  override suspend fun error(message: String) {
    add(LogMessage(loggerName = name, level = LogLevel.Error, message = message, ts = LocalDateTime.now()))
  }

  override suspend fun info(message: String) {
    add(LogMessage(loggerName = name, level = LogLevel.Info, message = message, ts = LocalDateTime.now()))
  }

  override suspend fun warning(message: String) {
    add(LogMessage(loggerName = name, level = LogLevel.Warning, message = message, ts = LocalDateTime.now()))
  }

  private suspend fun add(logMessage: LogMessage) {
    if (logMessage.level >= minLogLevel) {
      logMessages.emit(logMessage)
    }
  }
}
