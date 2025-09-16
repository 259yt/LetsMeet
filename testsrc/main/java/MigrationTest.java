import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import com.letsmeet.FullPostgresMigrator;


//Tests über Maven ausführen --> export JAVA_HOME=/opt/homebrew/opt/openjdk@17 && mvn test
public class MigrationTest {

    private static final String DB_URL = "jdbc:postgresql://localhost:5433/lf8_lets_meet_db";
    private static final String DB_USER = "user";
    private static final String DB_PASSWORD = "secret";

    @BeforeAll
    static void setup() {
        // Ensure the database is running and accessible before tests
        // This is a basic check, you might want to add more sophisticated checks
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            System.out.println("Database connection successful for testing.");
        } catch (Exception e) {
            System.err.println("Warning: Could not connect to the test database. Tests may fail.");
            e.printStackTrace();
        }
    }

    @Test
    void testImportExcelExecution() {
        // Capture stdout to verify execution messages
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outputStream));

        try {
            // Run the ImportExcel main method
            ImportExcel.main(new String[]{});

            // Restore original stdout
            System.setOut(originalOut);

            // Get the captured output
            String output = outputStream.toString();

            // Assertions
            assertTrue(output.contains("Starte Excel-Import..."), "Excel import should start with a message");
            assertTrue(output.contains("Excel-Import:"), "Excel import should complete with a summary message");
            assertFalse(output.contains("Import fehlgeschlagen"), "Excel import should not fail");
        } catch (Exception e) {
            // Restore original stdout in case of exception
            System.setOut(originalOut);
            fail("ImportExcel execution threw an exception: " + e.getMessage());
        }
    }

    @Test
    void testFullPostgresMigratorExecution() {
        // Capture stdout to verify execution messages
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outputStream));

        try {
            // Run the FullPostgresMigrator main method
            FullPostgresMigrator.main(new String[]{});

            // Restore original stdout
            System.setOut(originalOut);

            // Get the captured output
            String output = outputStream.toString();

            // Assertions
            assertTrue(output.contains("Verbindung zu MongoDB und PostgreSQL hergestellt."),
                "Migrator should establish database connections");
            assertTrue(output.contains("Vollständige Migration abgeschlossen."),
                "Migrator should complete with a success message");
            assertFalse(output.contains("Exception"), "Migrator execution should not throw exceptions");
        } catch (Exception e) {
            // Restore original stdout in case of exception
            System.setOut(originalOut);
            fail("FullPostgresMigrator execution threw an exception: " + e.getMessage());
        }
    }

    @Test
    void testFullMigrationProcess() {
        // This test runs both scripts sequentially to ensure the complete process works
        // Capture stdout to verify execution messages
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outputStream));

        try {
            // Step 1: Run ImportExcel
            ImportExcel.main(new String[]{});

            // Step 2: Run FullPostgresMigrator
            FullPostgresMigrator.main(new String[]{});

            // Restore original stdout
            System.setOut(originalOut);

            // Get the captured output
            String output = outputStream.toString();

            // Assertions
            assertTrue(output.contains("Starte Excel-Import..."), "Excel import should start");
            assertTrue(output.contains("Excel-Import:"), "Excel import should complete");
            assertTrue(output.contains("Verbindung zu MongoDB und PostgreSQL hergestellt."),
                "Migrator should establish connections");
            assertTrue(output.contains("Vollständige Migration abgeschlossen."),
                "Migrator should complete");

            // Check that no error messages are present
            assertFalse(output.contains("Import fehlgeschlagen"),
                "No import failure messages should be present");
            assertFalse(output.contains("Exception"),
                "No exception messages should be present");

        } catch (Exception e) {
            // Restore original stdout in case of exception
            System.setOut(originalOut);
            fail("Full migration process threw an exception: " + e.getMessage());
        }
    }
}
