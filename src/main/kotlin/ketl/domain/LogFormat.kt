package ketl.domain

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

fun defaultLogFormat(msg: LogMessage): String {
  val indentedMessage = msg.message.split("\n").joinToString("\n  ")
  return "${LocalDateTime.now().format(DateTimeFormatter.ofPattern("M/d @ hh:mm:ss a"))} - ${msg.loggerName} " +
    "[${msg.level.toString().uppercase()}]:\n  $indentedMessage"
}
