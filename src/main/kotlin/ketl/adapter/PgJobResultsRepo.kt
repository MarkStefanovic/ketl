package ketl.adapter

import ketl.domain.JobResult
import ketl.domain.JobResultsRepo
import ketl.domain.KETLErrror
import java.sql.Timestamp
import java.sql.Types
import java.time.LocalDateTime
import javax.sql.DataSource

data class PgJobResultsRepo(
  val schema: String,
  val showSQL: Boolean,
  private val ds: DataSource,
) : JobResultsRepo {
  init {
    ds.connection.use { connection ->
      connection.createStatement().use { statement ->
        // language=PostgreSQL
        val createTableSQL = """
          |  CREATE TABLE IF NOT EXISTS $schema.job_result (
          |    id SERIAL PRIMARY KEY
          |  , job_name TEXT NOT NULL CHECK (LENGTH(job_name) > 0)
          |  , start_time TIMESTAMP NOT NULL
          |  , end_time TIMESTAMP NOT NULL CHECK (end_time >= start_time)
          |  , result TEXT NOT NULL CHECK (
          |      result IN ('cancelled', 'failed', 'skipped', 'successful')
          |    )
          |  , error_message TEXT NULL CHECK (
          |      (result = 'failed' AND LENGTH(error_message) > 0)
          |      OR (result <> 'failed' AND error_message IS NULL)
          |    )
          |  , skip_reason TEXT NULL CHECK (
          |      (result = 'skipped' AND LENGTH(skip_reason) > 0)
          |      OR (result <> 'skipped' AND skip_reason IS NULL)
          |    )
          |  )
        """.trimMargin()

        if (showSQL) {
          println(
            """
            |PgJobResultsRepo.init createTableSQL:
            |  $createTableSQL 
          """.trimMargin()
          )
        }

        statement.execute(createTableSQL.trimIndent())

        // language=PostgreSQL
        val startTimeIndexSQL = """
          |-- noinspection SqlResolve @ table/"job_result"
          |  CREATE INDEX IF NOT EXISTS ix_job_result_job_name_start_time 
          |    ON $schema.job_result (job_name, start_time)
        """.trimMargin()

        statement.execute(startTimeIndexSQL.trimIndent())
      }
    }
  }

  override suspend fun add(result: JobResult) {
    // language=PostgreSQL
    val sql = """
      |  INSERT INTO $schema.job_result (
      |    job_name
      |  , start_time
      |  , end_time
      |  , result
      |  , error_message
      |  , skip_reason
      |  ) VALUES (
      |    ?
      |  , ?
      |  , ?
      |  , ?
      |  , ?
      |  , ?
      |)
    """.trimMargin()

    val resultName: String = when (result) {
      is JobResult.Cancelled -> "cancelled"
      is JobResult.Failed -> "failed"
      is JobResult.Skipped -> "skipped"
      is JobResult.Successful -> "successful"
    }

    val errorMessage: String? = if (result is JobResult.Failed) {
      result.errorMessage
    } else {
      null
    }

    val skipReason: String? = if (result is JobResult.Skipped) {
      result.reason
    } else {
      null
    }

    ds.connection.use { connection ->
      connection.prepareStatement(sql.trimIndent()).use { preparedStatement ->
        preparedStatement.setString(1, result.jobName)
        preparedStatement.setTimestamp(2, Timestamp.valueOf(result.start))
        preparedStatement.setTimestamp(3, Timestamp.valueOf(result.end))
        preparedStatement.setString(4, resultName)
        if (errorMessage == null) {
          preparedStatement.setNull(5, Types.VARCHAR)
        } else {
          preparedStatement.setString(5, errorMessage)
        }
        if (skipReason == null) {
          preparedStatement.setNull(6, Types.VARCHAR)
        } else {
          preparedStatement.setString(6, skipReason)
        }

        preparedStatement.executeUpdate()
      }
    }
  }

  override suspend fun deleteBefore(ts: LocalDateTime) {
    // language=PostgreSQL
    val sql = "DELETE FROM $schema.job_result WHERE start_time < ?"

    if (showSQL) {
      println(
        """
        |PgJobResultsRepo.deleteBefore SQL:
        |  $sql
      """.trimMargin()
      )
    }

    ds.connection.use { connection ->
      connection.prepareStatement(sql).use { preparedStatement ->
        preparedStatement.setTimestamp(1, Timestamp.valueOf(ts))

        preparedStatement.executeUpdate()
      }
    }
  }

  override suspend fun getLatestResults(): Set<JobResult> {
    // language=PostgreSQL
    val sql = """
      |-- noinspection SqlResolve @ schema/"jr"
      |  SELECT DISTINCT ON (jr.job_name)  
      |    jr.job_name
      |  , jr.start_time
      |  , jr.end_time
      |  , jr.result
      |  , jr.error_message
      |  , jr.skip_reason
      |  FROM $schema.job_result AS jr
      |  ORDER BY 
      |    jr.job_name
      |  , jr.start_time DESC
    """.trimMargin()

    if (showSQL) {
      println(
        """
        |PgJobResultsRepo.getLatestResult SQL:
        |  ${sql.split("\n").joinToString("\n    ")}
      """.trimMargin()
      )
    }

    ds.connection.use { connection ->
      connection.createStatement().use { statement ->
        val result = statement.executeQuery(sql)

        val jobResults = mutableListOf<JobResult>()
        while (result.next()) {
          val jobName = result.getString("job_name")
          val startTime = result.getTimestamp("start_time").toLocalDateTime()
          val endTime = result.getTimestamp("end_time").toLocalDateTime()
          val resultTypeName = result.getString("result")
          val errorMessage = result.getObject("error_message") as String?
          val skipReason = result.getObject("skip_reason") as String?

          val jobResult: JobResult = when (resultTypeName) {
            "cancelled" -> JobResult.Cancelled(
              jobName = jobName,
              start = startTime,
              end = endTime,
            )
            "failed" -> JobResult.Failed(
              jobName = jobName,
              start = startTime,
              end = endTime,
              errorMessage = errorMessage ?: "No message was provided.",
            )
            "skipped" -> JobResult.Skipped(
              jobName = jobName,
              start = startTime,
              end = endTime,
              reason = skipReason ?: "No reason was provided.",
            )
            "successful" -> JobResult.Successful(
              jobName = jobName,
              start = startTime,
              end = endTime,
            )
            else -> throw KETLErrror.UnrecognizedResultTypeName(resultTypeName)
          }

          jobResults.add(jobResult)
        }

        return jobResults.toSet()
      }
    }
  }
}
