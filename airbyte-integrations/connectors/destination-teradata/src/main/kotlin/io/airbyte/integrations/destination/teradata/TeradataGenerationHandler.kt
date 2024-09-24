/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.teradata

import io.airbyte.cdk.db.jdbc.JdbcDatabase
import io.airbyte.cdk.integrations.destination.jdbc.JdbcGenerationHandler
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class TeradataGenerationHandler : JdbcGenerationHandler {
    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(TeradataGenerationHandler::class.java)
    }
    override fun getGenerationIdInTable(
        database: JdbcDatabase,
        namespace: String,
        name: String
    ): Long {
        LOGGER.info("Satish : TeradataGenerationHandler- getGenerationIdInTable - db - {}, name - {}" , namespace, name)
        return 1
    }

}
