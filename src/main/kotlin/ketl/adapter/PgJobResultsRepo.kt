package ketl.adapter

import ketl.domain.JobResult
import ketl.domain.JobResultsRepo
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
        val fullTableName = """"$schema".job_result"""

        val createTableSQL = """
          |  CREATE TABLE IF NOT EXISTS $fullTableName (
          |    id SERIAL PRIMARY KEY
          |  , job_name TEXT NOT NULL CHECK (LENGTH(log_name) > 0)
          |  , start_time TIMESTAMP NOT NULL
          |  , end_time TIMESTAMP NOT NULL CHECK (end_time >= start_time)
          |  , result TEXT NOT NULL CHECK (result IN ('cancelled', 'failed', 'skipped', 'successful'))
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

        val startTimeIndexSQL = """
          |  CREATE INDEX IF NOT EXISTS ix_job_result_job_name_start_time 
          |    ON $fullTableName (job_name, start_time)
        """.trimMargin()

        statement.execute(startTimeIndexSQL.trimIndent())
      }
    }
  }

  override suspend fun add(result: JobResult) {
    val sql = """
      |  INSERT INTO "$schema".job_result (
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
      }
    }
  }

  override suspend fun deleteBefore(ts: LocalDateTime) {
    val sql = """DELETE FROM "$schema".job_result WHERE start_time < ?"""

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
      }
    }
  }
}
