package ketl.domain

abstract class JobContext(val log: LogMessages)

class BaseContext(log: LogMessages) : JobContext(log)
