import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Locale;

public class ExcelMigrator {

    // Datenbank-Konstanten
    private static final String PG_URL = "jdbc:postgresql://localhost:5433/lf8_lets_meet_db";
    private static final String PG_USER = "user";
    private static final String PG_PASSWORD = "secret";

    // Excel-Konstanten
    private static final String EXCEL_FILE_PATH = "Lets Meet DB Dump.xlsx";

    public static void main(String[] args) {
        System.out.println("Starte Excel-Datenmigration...");

        try (Connection pgConnection = DriverManager.getConnection(PG_URL, PG_USER, PG_PASSWORD)) {
            pgConnection.setAutoCommit(false); // Transaktionen aktivieren
            migrate(pgConnection);
            pgConnection.commit(); // Transaktion abschließen
            System.out.println("Excel-Datenmigration erfolgreich abgeschlossen.");
        } catch (SQLException e) {
            System.err.println("Datenbankfehler während der Excel-Migration: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Excel-Migration fehlgeschlagen: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void migrate(Connection pgConnection) throws Exception {
        int importedCount = 0;
        int skippedCount = 0;

        Path excelPath = Paths.get(EXCEL_FILE_PATH);
        if (!excelPath.isAbsolute()) {
            excelPath = Paths.get(System.getProperty("user.dir")).resolve(EXCEL_FILE_PATH).normalize();
        }
        if (!Files.exists(excelPath)) {
            throw new java.io.FileNotFoundException("Excel-Datei nicht gefunden: " + excelPath.toAbsolutePath());
        }

        try (FileInputStream fis = new FileInputStream(excelPath.toFile());
             Workbook workbook = new XSSFWorkbook(fis)) {
            Sheet sheet = workbook.getSheetAt(0);
            DataFormatter dataFormatter = new DataFormatter(Locale.GERMANY);
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("d.M.uuuu").withResolverStyle(ResolverStyle.STRICT);

            for (Row row : sheet) {
                if (row.getRowNum() == 0) continue; // Kopfzeile überspringen

                try {
                    String[] nameParts = row.getCell(0).getStringCellValue().split(", ");
                    String nachname = nameParts[0].trim();
                    String vorname = nameParts.length > 1 ? nameParts[1].trim() : "";
                    String userEmail = row.getCell(4).getStringCellValue().trim();

                    // Benutzer einfügen oder aktualisieren (COALESCE, um Informationen zu ergänzen)
                    String insertUserSql = "INSERT INTO app_user (user_email, first_name, last_name, gender, date_of_birth) " +
                            "VALUES (?, ?, ?, ?, ?) " +
                            "ON CONFLICT (user_email) DO UPDATE SET " +
                            "first_name = COALESCE(EXCLUDED.first_name, app_user.first_name), " +
                            "last_name = COALESCE(EXCLUDED.last_name, app_user.last_name), " +
                            "gender = COALESCE(EXCLUDED.gender, app_user.gender), " +
                            "date_of_birth = COALESCE(EXCLUDED.date_of_birth, app_user.date_of_birth)";

                    try (PreparedStatement stmt = pgConnection.prepareStatement(insertUserSql)) {
                        stmt.setString(1, userEmail);
                        stmt.setString(2, vorname);
                        stmt.setString(3, nachname);
                        stmt.setString(4, row.getCell(5).getStringCellValue()); // gender

                        LocalDate birthDate = null;
                        Cell dateCell = row.getCell(7);
                        if (dateCell != null && dateCell.getCellType() == CellType.NUMERIC && DateUtil.isCellDateFormatted(dateCell)) {
                            java.util.Date utilDate = DateUtil.getJavaDate(dateCell.getNumericCellValue());
                            birthDate = utilDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                        } else if (dateCell != null) {
                            try {
                                String raw = dataFormatter.formatCellValue(dateCell).trim();
                                if (!raw.isEmpty()) {
                                    birthDate = LocalDate.parse(raw, dateFormatter);
                                }
                            } catch (DateTimeParseException ex) {
                                // Datum konnte nicht geparst werden
                            }
                        }
                        if (birthDate != null) {
                            stmt.setDate(5, java.sql.Date.valueOf(birthDate));
                        } else {
                            stmt.setNull(5, java.sql.Types.DATE);
                        }
                        stmt.executeUpdate();
                    }

                    // Telefonnummer separat speichern
                    String phoneSql = "INSERT INTO phone_number (phone_number, user_email) VALUES (?, ?) ON CONFLICT (phone_number) DO NOTHING";
                    try (PreparedStatement stmt = pgConnection.prepareStatement(phoneSql)) {
                        stmt.setString(1, row.getCell(2).getStringCellValue().trim());
                        stmt.setString(2, userEmail);
                        stmt.executeUpdate();
                    }

                    // Hobbys verarbeiten
                    String[] hobbies = row.getCell(3) != null ? row.getCell(3).getStringCellValue().split(";") : new String[0];
                    for (String hobbyStr : hobbies) {
                        if (hobbyStr == null || hobbyStr.trim().isEmpty()) continue;
                        hobbyStr = hobbyStr.trim();
                        String[] hobbyParts = hobbyStr.split("%");
                        String hobbyName = hobbyParts[0].trim();
                        int prio = 0;
                        if (hobbyParts.length > 1) {
                            try {
                                prio = Integer.parseInt(hobbyParts[1].trim());
                            } catch (NumberFormatException ex) {
                                prio = 0;
                            }
                        }

                        // Hobby einfügen oder ignorieren, wenn es schon existiert
                        String insertHobbySql = "INSERT INTO hobby (name) VALUES (?) ON CONFLICT (name) DO NOTHING";
                        try (PreparedStatement insertHobby = pgConnection.prepareStatement(insertHobbySql)) {
                            insertHobby.setString(1, hobbyName);
                            insertHobby.executeUpdate();
                        }

                        // Verknüpfung speichern/aktualisieren
                        String linkHobbySql = "INSERT INTO user_hobby (user_email, hobby_id, priority) VALUES (?, (SELECT hobby_id FROM hobby WHERE name = ?), ?) ON CONFLICT (user_email, hobby_id) DO UPDATE SET priority = EXCLUDED.priority";
                        try (PreparedStatement linkStmt = pgConnection.prepareStatement(linkHobbySql)) {
                            linkStmt.setString(1, userEmail);
                            linkStmt.setString(2, hobbyName);
                            linkStmt.setInt(3, prio);
                            linkStmt.executeUpdate();
                        }
                    }
                    importedCount++;
                } catch (SQLException e) {
                    if ("23505".equals(e.getSQLState())) {
                        skippedCount++;
                    } else {
                        throw e;
                    }
                }
            }
            System.out.printf("Excel-Import: %d Benutzer migriert (%d übersprungen).%n", importedCount, skippedCount);
        }
    }
}
