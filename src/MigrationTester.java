import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MigrationTester {

    // Datenbank-Konstanten
    private static final String PG_URL = "jdbc:postgresql://localhost:5433/lf8_lets_meet_db";
    private static final String PG_USER = "user";
    private static final String PG_PASSWORD = "secret";

    public static void main(String[] args) {
        System.out.println("Starte Migrationstests...");

        try (Connection pgConnection = DriverManager.getConnection(PG_URL, PG_USER, PG_PASSWORD)) {
            testUserCount(pgConnection);
            testStefanJasperUserData(pgConnection);
            testMongoUser(pgConnection);
            testStefanJasperHobbies(pgConnection);
            testStefanJasperHobbyPriorities(pgConnection);
            testConversationsAndMessages(pgConnection);
            testStefanJasperMessages(pgConnection);
        } catch (Exception e) {
            System.err.println("Ein Fehler ist während des Tests aufgetreten: " + e.getMessage());
            e.printStackTrace();
        } finally {
            System.out.println("\nAlle Tests abgeschlossen.");
        }
    }

    private static void testUserCount(Connection pgConnection) throws Exception {
        System.out.println("--- Test 1: Überprüfe die Gesamtzahl der Benutzer ---");
        String sql = "SELECT COUNT(*) FROM app_user";
        try (Statement stmt = pgConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                int count = rs.getInt(1);
                System.out.println("Anzahl der Benutzer in der Datenbank: " + count);
                if (count == 1576) {
                    System.out.println("Test 1: OK");
                } else {
                    System.err.println("Test 1: FEHLGESCHLAGEN. Erwartet: 1576, Gefunden: " + count);
                }
            }
        }
        System.out.flush();
    }

    private static void testStefanJasperUserData(Connection pgConnection) throws Exception {
        System.out.println("\n--- Test 2: Überprüfe Benutzerdaten (Stefan Jasper) ---");
        String email = "stefan.jasper@gmaiil.te";
        String sql = "SELECT first_name, last_name, gender, date_of_birth FROM app_user WHERE user_email = '" + email + "'";
        try (Statement stmt = pgConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                String firstName = rs.getString("first_name");
                String lastName = rs.getString("last_name");
                String gender = rs.getString("gender");
                Date dobSql = rs.getDate("date_of_birth");
                LocalDate dob = (dobSql != null) ? dobSql.toLocalDate() : null;

                if ("Stefan".equals(firstName) && "Jasper".equals(lastName) && "m".equals(gender) && LocalDate.of(1967, 7, 14).equals(dob)) {
                    System.out.println("Test 2: OK. Benutzerdaten korrekt.");
                } else {
                    System.err.println("Test 2: FEHLGESCHLAGEN. Unerwartete Daten.");
                    System.err.println("Erwartet: Name='Stefan Jasper', Geschlecht='m', Geburtsdatum='1967-07-14'");
                    System.err.println("Gefunden: Name='" + firstName + " " + lastName + "', Geschlecht='" + gender + "', Geburtsdatum='" + dob + "'");
                }
            } else {
                System.err.println("Test 2: FEHLGESCHLAGEN. Benutzer wurde nicht gefunden.");
            }
        }
        System.out.flush();
    }

    private static void testMongoUser(Connection pgConnection) throws Exception {
        System.out.println("\n--- Test 3: Überprüfe die migrierten Likes (Stefan Jasper) in PostgreSQL ---");
        String email = "stefan.jasper@gmaiil.te";
        int likeCount = 0;

        String sql = "SELECT COUNT(*) FROM interest WHERE from_user_id = '" + email + "'";
        try (Statement stmt = pgConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                likeCount = rs.getInt(1);
            }
        }

        System.out.println("Gefundene Likes für " + email + ": " + likeCount);
        if (likeCount == 0) {
            System.out.println("Test 3: OK");
        } else {
            System.err.println("Test 3: FEHLGESCHLAGEN. Erwartet: 0 Likes. Gefunden: " + likeCount);
        }
        System.out.flush();
    }

    private static void testStefanJasperHobbies(Connection pgConnection) throws Exception {
        System.out.println("\n--- Test 4: Überprüfe Hobbys (Stefan Jasper) ---");
        String email = "stefan.jasper@gmaiil.te";
        String sql = "SELECT h.name FROM user_hobby uh JOIN hobby h ON uh.hobby_id = h.hobby_id WHERE uh.user_email = '" + email + "'";
        try (Statement stmt = pgConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            List<String> hobbies = new ArrayList<>();
            while (rs.next()) {
                hobbies.add(rs.getString("name"));
            }
            System.out.println("Gefundene Hobbys:");
            hobbies.forEach(h -> System.out.println("- " + h));

            List<String> expectedHobbies = Arrays.asList(
                    "Netflix chillen",
                    "Romane, Erzählungen, Theaterstücke oder Gedichte schreiben",
                    "Camping",
                    "Schreiben",
                    "Angeln"
            );

            if (hobbies.size() == expectedHobbies.size() && hobbies.containsAll(expectedHobbies)) {
                System.out.println("Test 4: OK. Alle 5 Hobbys korrekt migriert.");
            } else {
                System.err.println("Test 4: FEHLGESCHLAGEN. Erwartet: " + expectedHobbies.size() + " Hobbys, Gefunden: " + hobbies.size());
            }
        }
        System.out.flush();
    }

    private static void testStefanJasperHobbyPriorities(Connection pgConnection) throws Exception {
        System.out.println("\n--- Test 5: Überprüfe Hobby-Prioritäten (Stefan Jasper) ---");
        String email = "stefan.jasper@gmaiil.te";

        // Test für Excel-Priorität (mit %-Wert)
        String sqlExcelHobby = "SELECT priority FROM user_hobby uh JOIN hobby h ON uh.hobby_id = h.hobby_id WHERE uh.user_email = '" + email + "' AND h.name = 'Netflix chillen'";
        try (Statement stmt = pgConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sqlExcelHobby)) {
            if (rs.next()) {
                int priority = rs.getInt("priority");
                System.out.println("Prioritätswert für 'Netflix chillen': " + priority);
                if (priority == 33) {
                    System.out.println("Test 5a: OK. Excel-Priorität korrekt.");
                } else {
                    System.err.println("Test 5a: FEHLGESCHLAGEN. Erwartet: 33, Gefunden: " + priority);
                }
            } else {
                System.err.println("Test 5a: FEHLGESCHLAGEN. Hobby 'Netflix chillen' nicht gefunden.");
            }
        }

        // Test für XML-Hobby (ohne %-Wert)
        String sqlXmlHobby = "SELECT priority FROM user_hobby uh JOIN hobby h ON uh.hobby_id = h.hobby_id WHERE uh.user_email = '" + email + "' AND h.name = 'Camping'";
        try (Statement stmt = pgConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sqlXmlHobby)) {
            if (rs.next()) {
                Object priority = rs.getObject("priority");
                System.out.println("Prioritätswert für 'Camping': " + priority);
                if (priority == null) {
                    System.out.println("Test 5b: OK. XML-Priorität korrekt (NULL-Wert).");
                } else {
                    System.err.println("Test 5b: FEHLGESCHLAGEN. Priorität sollte NULL sein.");
                }
            } else {
                System.err.println("Test 5b: FEHLGESCHLAGEN. Hobby 'Camping' nicht gefunden.");
            }
        }
        System.out.flush();
    }

    private static void testConversationsAndMessages(Connection pgConnection) throws Exception {
        System.out.println("\n--- Test 6: Überprüfe Konversationen und Nachrichten (ID 36) ---");
        String convSql = "SELECT date_of_creation FROM conversation WHERE conversation_id = 36";
        try (Statement stmt = pgConnection.createStatement();
             ResultSet rs = stmt.executeQuery(convSql)) {
            if (rs.next()) {
                System.out.println("Konversation (ID 36) gefunden. Erstellungsdatum: " + rs.getDate("date_of_creation"));
            } else {
                System.err.println("Test 6: FEHLGESCHLAGEN. Konversation (ID 36) nicht gefunden.");
                return;
            }
        }

        String msgSql = "SELECT sender_id, receiver_id, text, date_of_sending FROM message WHERE conversation_id = 36 ORDER BY date_of_sending ASC LIMIT 1";
        try (Statement stmt = pgConnection.createStatement();
             ResultSet rs = stmt.executeQuery(msgSql)) {
            if (rs.next()) {
                String sender = rs.getString("sender_id");
                String receiver = rs.getString("receiver_id");
                String text = rs.getString("text");
                java.sql.Date date = rs.getDate("date_of_sending");
                System.out.println("Erste Nachricht: Sender='" + sender + "', Empfänger='" + receiver + "', Text='" + text + "', Datum='" + date + "'");
                if ("bernt.jakobtorweihe@a-o-l.te".equals(sender) && "johanna.krüsmann@web.ork".equals(receiver) && "Hast du am Wochenende Zeit?".equals(text)) {
                    System.out.println("Test 6: OK");
                } else {
                    System.err.println("Test 6: FEHLGESCHLAGEN. Nachrichtendetails stimmen nicht überein.");
                }
            } else {
                System.err.println("Test 6: FEHLGESCHLAGEN. Nachrichten für Konversation (ID 36) nicht gefunden.");
            }
        }
        System.out.flush();
    }

    private static void testStefanJasperMessages(Connection pgConnection) throws Exception {
        System.out.println("\n--- Test 7: Überprüfe Nachrichten (Stefan Jasper) ---");
        String msgSql = "SELECT sender_id, receiver_id, text FROM message WHERE conversation_id = 34 AND sender_id = 'stefan.jasper@gmaiil.te' ORDER BY date_of_sending ASC LIMIT 1";
        try (Statement stmt = pgConnection.createStatement();
             ResultSet rs = stmt.executeQuery(msgSql)) {
            if (rs.next()) {
                String sender = rs.getString("sender_id");
                String receiver = rs.getString("receiver_id");
                String text = rs.getString("text");

                if ("stefan.jasper@gmaiil.te".equals(sender) && "vera.stute@d-ohnline.te".equals(receiver) && "Was hältst du davon, wenn wir uns bald wiedersehen?".equals(text)) {
                    System.out.println("Test 7: OK. Nachrichtendetails korrekt.");
                } else {
                    System.err.println("Test 7: FEHLGESCHLAGEN. Unerwartete Nachrichtendetails.");
                }
            } else {
                System.err.println("Test 7: FEHLGESCHLAGEN. Nachricht für Konversation (ID 34) nicht gefunden.");
            }
        }
        System.out.flush();
    }
}
