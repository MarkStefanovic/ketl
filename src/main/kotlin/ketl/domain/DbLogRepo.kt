package ketl.domain

import java.time.LocalDateTime

interface DbLogRepo {
  suspend fun add(message: LogMessage)

  suspend fun createTable()

  suspend fun deleteBefore(ts: LocalDateTime)
}
