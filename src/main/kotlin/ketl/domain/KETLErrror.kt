package ketl.domain

import kotlin.time.Duration

sealed class KETLErrror(errorMessage: String) : Exception(errorMessage) {
  data class JobResultsStopped(val timeSinceLatestResult: Duration) : KETLErrror(
    "It has been more than ${timeSinceLatestResult.inWholeSeconds} seconds since the last results were received.  " +
      "Something must have went wrong."
  )

  data class UnrecognizedResultTypeName(val resultTypeName: String) : KETLErrror(
    "The result type name, $resultTypeName, is not recognized.  " +
      "Recognized result type name include the following: 'cancelled', 'failed', 'skipped', 'successful'"
  )

  data class UnrecognizedStatusName(val statusName: String) : KETLErrror(
    "The status name, $statusName, is not recognized.  " +
      "Recognized status names include the following:  'cancelled', 'initial', 'running', 'skipped', 'successful', 'failed'."
  )
}
