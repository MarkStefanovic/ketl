package ketl.domain

sealed class KETLErrror(errorMessage: String) : Exception(errorMessage) {
  data class UnrecognizedResultTypeName(val resultTypeName: String) : KETLErrror(
    "The result type name, $resultTypeName, is not recognized.  " +
      "Recognized result type name include the following: 'cancelled', 'failed', 'skipped', 'successful'"
  )
}
