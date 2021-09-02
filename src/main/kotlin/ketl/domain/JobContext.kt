package ketl.domain

import java.io.Closeable

abstract class JobContext(val log: LogMessages) : Closeable

class BaseContext(log: LogMessages) : JobContext(log) {
  override fun close() {}
}
