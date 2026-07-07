package com.arrowflight.coordinator;

import java.util.Properties;
import java.util.Map;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;

final class CoordinatorMigrations {
    private CoordinatorMigrations() {
    }

    static void migrate(Config config) {
        if (!config.metadataMigrationsEnabled) {
            CoordinatorLog.info("coordinator_migrations_skipped", Map.of(
                    "reason", "disabled"
            ));
            return;
        }
        if (config.metadataDatabaseUrl.isEmpty()) {
            CoordinatorLog.info("coordinator_migrations_skipped", Map.of(
                    "reason", "metadata database is not configured"
            ));
            return;
        }

        JdbcTarget target = CoordinatorMetadataStore.parseJdbcTarget(config.metadataDatabaseUrl.get());
        Properties properties = target.properties();
        Flyway flyway = Flyway.configure()
                .dataSource(
                        target.jdbcUrl(),
                        properties.getProperty("user"),
                        properties.getProperty("password")
                )
                .locations("classpath:db/migration")
                .baselineOnMigrate(config.metadataMigrationsBaselineOnMigrate)
                .baselineVersion("0")
                .load();

        MigrateResult result = flyway.migrate();
        CoordinatorLog.info("coordinator_migrations_complete", Map.of(
                "migrationsExecuted", result.migrationsExecuted
        ));
    }
}
