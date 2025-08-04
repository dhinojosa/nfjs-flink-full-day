package com.evolutionnext;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkTableTest {

    @Test
    public void testFlinkTableAPIWithSelect() {
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .build();

        TableEnvironment tableEnvironment =
            TableEnvironment.create(settings);

        // Create a sample data table
        Table exampleTable = tableEnvironment.fromValues(
            DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("name", DataTypes.STRING())),
            Row.of(1, "Alice"),
            Row.of(2, "Bob"),
            Row.of(3, "Charlie")
        );

        // Perform a simple transformation (e.g., a projection)
        Table transformedTable =
            exampleTable.select($("id"), $("name"));

        try (CloseableIterator<Row> iterator =
                 transformedTable.execute().collect()) {
            iterator.forEachRemaining(r -> System.out.printf("id: %d, name: %s%n", r.<Integer>getFieldAs("id"), r.getField("name")));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testFlinkTableAPIWithSQLWithDataGen() {
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .build();

        TableEnvironment tableEnvironment =
            TableEnvironment.create(settings);

        // Register an inline table using SQL
        tableEnvironment.executeSql(
            "CREATE TEMPORARY TABLE example_table (id INT, name STRING) WITH (" +
            "  'connector' = 'datagen',\n " +
            "  'fields.id.kind' = 'sequence',\n" +
            "  'fields.id.start' = '1',\n" +
            "  'fields.id.end' = '1000',\n" +
            "  'rows-per-second' = '10')"
        );

        // Perform a SQL transformation
        Table transformedTable = tableEnvironment.sqlQuery(
            "SELECT id, name FROM example_table;"
        );

        // Collect results
        try (CloseableIterator<Row> iterator = transformedTable.execute().collect()) {
            iterator.forEachRemaining(r ->
                System.out.printf("id: %d, name: %s%n",
                    r.<Integer>getFieldAs("id"), r.getField("name")));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
