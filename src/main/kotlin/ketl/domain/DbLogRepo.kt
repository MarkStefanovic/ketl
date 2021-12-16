package ketl.domain

import java.time.LocalDateTime

interface DbLogRepo {
  fun add(message: LogMessage)

  fun createTable()

  fun deleteBefore(ts: LocalDateTime)
}
