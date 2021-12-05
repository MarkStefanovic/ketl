package ketl.domain

import java.time.LocalDateTime

data class LogMessage(
  val loggerName: String,
  val level: LogLevel,
  val message: String,
  val ts: LocalDateTime,
) {
  override fun toString() = """
    |LogMessage [
    |  loggerName: $loggerName
    |  level: $level
    |  message: $message
    |  ts: $ts
    |]
  """.trimMargin()
}
