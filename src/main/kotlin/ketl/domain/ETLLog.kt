package ketl.domain

interface ETLLog {
  suspend fun debug(message: String)

  suspend fun error(message: String)

  suspend fun info(message: String)

  suspend fun warning(message: String)
}

class RootLog(private val log: SharedLog) : ETLLog {
  override suspend fun debug(message: String) {
    log.debug(name = "ketl", message = message)
  }

  override suspend fun error(message: String) {
    log.error(name = "ketl", message = message)
  }

  override suspend fun info(message: String) {
    log.info(name = "ketl", message = message)
  }

  override suspend fun warning(message: String) {
    log.warning(name = "ketl", message = message)
  }
}

class JobLog(private val jobName: String, private val log: SharedLog) : ETLLog {
  override suspend fun debug(message: String) {
    log.debug(name = jobName, message = message)
  }

  override suspend fun error(message: String) {
    log.error(name = jobName, message = message)
  }

  override suspend fun info(message: String) {
    log.info(name = jobName, message = message)
  }

  override suspend fun warning(message: String) {
    log.warning(name = jobName, message = message)
  }
}
