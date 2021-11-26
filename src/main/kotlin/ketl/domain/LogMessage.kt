package ketl.domain

data class LogMessage(
  val loggerName: String,
  val level: LogLevel,
  val message: String,
) {
  override fun toString() = """
    |LogMessage [
    |  loggerName: $loggerName
    |  level: $level
    |  message: $message
    |]
  """.trimMargin()
}
