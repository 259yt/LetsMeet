import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.nio.file.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class ImportExcel {
    private static final String DB_URL = "jdbc:postgresql://localhost:5433/lf8_lets_meet_db";
    private static final String DB_USER = "user";
    private static final String DB_PASSWORD = "secret";
    private static final String EXCEL_FILE_PATH = "Lets Meet DB Dump.xlsx";

    public static void main(String[] args) {
        System.out.println("Starte Excel-Import...");

        int importedCount = 0;
        int skippedCount = 0;

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            // Zuerst alle relevanten Tabellen leeren, um Duplikate zu vermeiden
            System.out.println("Lösche vorhandene Daten...");
            conn.prepareStatement("DELETE FROM message").executeUpdate();
            conn.prepareStatement("DELETE FROM conversation").executeUpdate();
            conn.prepareStatement("DELETE FROM interest").executeUpdate();
            conn.prepareStatement("DELETE FROM friendship").executeUpdate();
            conn.prepareStatement("DELETE FROM user_hobby").executeUpdate();
            conn.prepareStatement("DELETE FROM phone_number").executeUpdate();
            conn.prepareStatement("DELETE FROM hobby").executeUpdate();
            conn.prepareStatement("DELETE FROM app_user").executeUpdate();
            System.out.println("Alle vorhandenen Benutzerdaten gelöscht.");

            Path excelPath = Paths.get(EXCEL_FILE_PATH);
            if (!excelPath.isAbsolute()) {
                excelPath = Paths.get(System.getProperty("user.dir"))
                        .resolve(EXCEL_FILE_PATH)
                        .normalize();
            }
            if (!Files.exists(excelPath)) {
                throw new java.io.FileNotFoundException(excelPath + " (not found). Arbeitsverzeichnis: " + System.getProperty("user.dir"));
            }
            System.out.println("Lese Excel-Datei von: " + excelPath.toAbsolutePath());
            FileInputStream fis = new FileInputStream(excelPath.toFile());
            Workbook workbook = new XSSFWorkbook(fis);
            Sheet sheet = workbook.getSheetAt(0);
            DataFormatter dataFormatter = new DataFormatter(Locale.GERMANY);
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("d.M.uuuu");

            // Überschrift überspringen (erste Zeile)
            Iterator<Row> rowIterator = sheet.iterator();
            if (rowIterator.hasNext()) rowIterator.next();

            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                try {
                    // Debug: Ausgabe der Zeilennummer und Zellinhalte
                    System.out.println("Verarbeite Zeile: " + row.getRowNum());
                    
                    // Überprüfe, ob die Zellen existieren und nicht leer sind
                    Cell nameCell = row.getCell(0);
                    if (nameCell == null || nameCell.getCellType() == CellType.BLANK) {
                        System.out.println("Zeile " + row.getRowNum() + " übersprungen: Name-Zelle ist leer");
                        skippedCount++;
                        continue;
                    }
                    
                    String[] nameParts = nameCell.getStringCellValue().split(", ");
                    String nachname = nameParts[0].trim();
                    String vorname = nameParts.length > 1 ? nameParts[1].trim() : "";

                    // Überprüfe E-Mail-Zelle
                    Cell emailCell = row.getCell(4);
                    if (emailCell == null || emailCell.getCellType() == CellType.BLANK) {
                        System.out.println("Zeile " + row.getRowNum() + " übersprungen: E-Mail-Zelle ist leer");
                        skippedCount++;
                        continue;
                    }
                    
                    String userEmail = emailCell.getStringCellValue();
                    System.out.println("Verarbeite Benutzer: " + userEmail);

                    String[] addressParts = row.getCell(1).getStringCellValue().split(", ");
                    String strasse = addressParts[0].trim();
                    String plz = addressParts.length > 1 ? addressParts[1].trim() : "";
                    String ort = addressParts.length > 2 ? addressParts[2].trim() : "";

                    LocalDate birthDate = null;
                    Cell dateCell = row.getCell(7);
                    if (dateCell != null) {
                        try {
                            if (dateCell.getCellType() == CellType.NUMERIC && DateUtil.isCellDateFormatted(dateCell)) {
                                birthDate = dateCell.getLocalDateTimeCellValue().toLocalDate();
                            } else {
                                String raw = dataFormatter.formatCellValue(dateCell).trim();
                                if (!raw.isEmpty()) {
                                    birthDate = LocalDate.parse(raw, dateFormatter);
                                }
                            }
                        } catch (DateTimeParseException ex) {
                            System.err.println("Ungültiges Datumsformat in Zeile " + row.getRowNum() + ": " + ex.getMessage());
                        } catch (Exception ex) {
                            System.err.println("Fehler beim Parsen des Datums in Zeile " + row.getRowNum() + ": " + ex.getMessage());
                        }
                    }
                    if (birthDate == null) {
                        System.out.println("Zeile " + row.getRowNum() + " übersprungen: Kein gültiges Geburtsdatum");
                        skippedCount++;
                        continue; // Zeile ohne gültiges Geburtsdatum überspringen
                    }

                    // Benutzer einfügen (gemäß Schema: app_user)
                    String insertUserSql = """
                        INSERT INTO app_user (user_email, first_name, last_name, gender, date_of_birth)
                        VALUES (?, ?, ?, ?, ?)
                        """;

                    try (PreparedStatement stmt = conn.prepareStatement(insertUserSql)) {
                        stmt.setString(1, userEmail);
                        stmt.setString(2, vorname);
                        stmt.setString(3, nachname);
                        
                        // Gender-Feld sicher behandeln
                        String gender = null;
                        if (row.getCell(5) != null && row.getCell(5).getCellType() != CellType.BLANK) {
                            gender = row.getCell(5).getStringCellValue();
                        }
                        stmt.setString(4, gender); // gender (kann null sein)
                        stmt.setDate(5, Date.valueOf(birthDate)); // date_of_birth
                        stmt.executeUpdate();
                        System.out.println("Benutzer " + userEmail + " erfolgreich eingefügt");
                    }

                    // Telefonnummer separat speichern (gemäß Schema: phone_number)
                    String phoneSql = "INSERT INTO phone_number (phone_number, user_email) VALUES (?, ?)";
                    try (PreparedStatement stmt = conn.prepareStatement(phoneSql)) {
                        String phone = "";
                        if (row.getCell(2) != null && row.getCell(2).getCellType() != CellType.BLANK) {
                            phone = row.getCell(2).getStringCellValue();
                        }
                        stmt.setString(1, phone);
                        stmt.setString(2, userEmail);
                        stmt.executeUpdate();
                    }

                    // Hobbys verarbeiten (gemäß Schema: hobby, user_hobby)
                    String[] hobbies = new String[0];
                    if (row.getCell(3) != null && row.getCell(3).getCellType() != CellType.BLANK) {
                        hobbies = row.getCell(3).getStringCellValue().split(";");
                    }
                    
                    for (String hobbyStr : hobbies) {
                        if (hobbyStr == null) continue;
                        hobbyStr = hobbyStr.trim();
                        if (hobbyStr.isEmpty()) continue;

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

                        // Hobby-ID ermitteln oder anlegen
                        Integer hobbyId = null;
                        try (PreparedStatement findHobby = conn.prepareStatement("SELECT hobby_id FROM hobby WHERE name = ? LIMIT 1")) {
                            findHobby.setString(1, hobbyName);
                            try (ResultSet rs = findHobby.executeQuery()) {
                                if (rs.next()) hobbyId = rs.getInt("hobby_id");
                            }
                        }
                        if (hobbyId == null) {
                            try (PreparedStatement nextIdStmt = conn.prepareStatement("SELECT COALESCE(MAX(hobby_id), 0) + 1 AS next_id FROM hobby");
                                 ResultSet rs = nextIdStmt.executeQuery()) {
                                rs.next();
                                hobbyId = rs.getInt("next_id");
                            }
                            try (PreparedStatement insertHobby = conn.prepareStatement("INSERT INTO hobby (hobby_id, name) VALUES (?, ?)")) {
                                insertHobby.setInt(1, hobbyId);
                                insertHobby.setString(2, hobbyName);
                                insertHobby.executeUpdate();
                            }
                        }

                        // Verknüpfung speichern/aktualisieren
                        try (PreparedStatement linkStmt = conn.prepareStatement(
                                "INSERT INTO user_hobby (user_email, hobby_id, priority) VALUES (?, ?, ?)")) {
                            linkStmt.setString(1, userEmail);
                            linkStmt.setInt(2, hobbyId);
                            linkStmt.setInt(3, prio);
                            linkStmt.executeUpdate();
                        }
                    }
                    importedCount++;
                } catch (SQLException e) {
                    System.err.println("SQL-Fehler in Zeile " + row.getRowNum() + ": " + e.getMessage());
                    e.printStackTrace();
                    skippedCount++;
                } catch (Exception e) {
                    System.err.println("Allgemeiner Fehler in Zeile " + row.getRowNum() + ": " + e.getMessage());
                    e.printStackTrace();
                    skippedCount++;
                }
            }

            System.out.printf("Excel-Import: %d Benutzer importiert (%d übersprungen).%n", importedCount, skippedCount);

        } catch (Exception e) {
            System.err.println("Import fehlgeschlagen: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
