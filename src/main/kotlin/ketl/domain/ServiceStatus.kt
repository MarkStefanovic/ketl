package ketl.domain

sealed class ServiceStatus {
  object Initial: ServiceStatus()

  data class Error(val message: String, val originalError: Throwable): ServiceStatus()

  object Ok: ServiceStatus()
}
