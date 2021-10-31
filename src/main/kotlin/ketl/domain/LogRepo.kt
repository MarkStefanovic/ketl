package ketl.domain

import java.time.LocalDateTime

interface LogRepo {
  fun add(message: LogMessage)

  fun deleteBefore(ts: LocalDateTime)
}
