package main.kotlin.ketl.domain

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class LogMessages(private val name: String) {
  private val _stream =
    MutableSharedFlow<LogMessage>(
      onBufferOverflow = BufferOverflow.DROP_OLDEST,
      extraBufferCapacity = 10,
    )

  val stream = _stream.asSharedFlow()

  suspend fun debug(message: String) {
    _stream.emit(
      LogMessage(
        loggerName = name,
        level = LogLevel.Debug,
        message = message,
      )
    )
  }

  suspend fun error(message: String) {
    _stream.emit(
      LogMessage(
        loggerName = name,
        level = LogLevel.Error,
        message = message,
      )
    )
  }

  suspend fun info(message: String) {
    _stream.emit(
      LogMessage(
        loggerName = name,
        level = LogLevel.Info,
        message = message,
      )
    )
  }

  suspend fun warning(message: String) {
    _stream.emit(
      LogMessage(
        loggerName = name,
        level = LogLevel.Warning,
        message = message,
      )
    )
  }
}

fun defaultLogFormat(msg: LogMessage): String =
  "${LocalDateTime.now().format(DateTimeFormatter.ofPattern("M/d @ hh:mm:ss a"))} " +
    "[${msg.loggerName}] ${msg.level.toString().uppercase()}: ${msg.message}"
