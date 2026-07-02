package com.arrowflight.coordinator;

import java.util.Properties;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;

final class CoordinatorMigrations {
    private CoordinatorMigrations() {
    }

    static void migrate(Config config) {
        if (!config.metadataMigrationsEnabled) {
            System.out.println("coordinator metadata migrations disabled");
            return;
        }
        if (config.metadataDatabaseUrl.isEmpty()) {
            System.out.println("coordinator metadata migrations skipped: metadata database is not configured");
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
        System.out.printf(
                "coordinator metadata migrations complete: executed=%d%n",
                result.migrationsExecuted
        );
    }
}
