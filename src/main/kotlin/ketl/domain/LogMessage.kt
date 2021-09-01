package ketl.domain

data class LogMessage(
  val loggerName: String,
  val level: LogLevel,
  val message: String,
  val thread: String,
)
