package ketl.domain

import java.io.Closeable

abstract class JobContext : Closeable {
  fun failure(reason: String) = Status.Failure(reason)

  fun skipped(reason: String) = Status.Skipped(reason)

  fun success() = Status.Success
}
