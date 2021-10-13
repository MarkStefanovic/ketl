package ketl.domain

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class SharedLog {
  private val _stream =
    MutableSharedFlow<LogMessage>(
      onBufferOverflow = BufferOverflow.DROP_OLDEST,
      extraBufferCapacity = 10,
    )

  val stream = _stream.asSharedFlow()

  suspend fun debug(name: String, message: String) {
    _stream.emit(
      LogMessage(
        loggerName = name,
        level = LogLevel.Debug,
        message = message,
        thread = Thread.currentThread().name,
      )
    )
  }

  suspend fun error(name: String, message: String) {
    _stream.emit(
      LogMessage(
        loggerName = name,
        level = LogLevel.Error,
        message = message,
        thread = Thread.currentThread().name,
      )
    )
  }

  suspend fun info(name: String, message: String) {
    _stream.emit(
      LogMessage(
        loggerName = name,
        level = LogLevel.Info,
        message = message,
        thread = Thread.currentThread().name,
      )
    )
  }

  suspend fun warning(name: String, message: String) {
    _stream.emit(
      LogMessage(
        loggerName = name,
        level = LogLevel.Warning,
        message = message,
        thread = Thread.currentThread().name,
      )
    )
  }
}

fun defaultLogFormat(msg: LogMessage): String =
  "${LocalDateTime.now().format(DateTimeFormatter.ofPattern("M/d @ hh:mm:ss a"))} " +
    "${msg.level.toString().uppercase()} (${msg.thread}): ${msg.message}"
