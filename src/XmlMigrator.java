import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class XmlMigrator {

    // Datenbank-Konstanten
    private static final String PG_URL = "jdbc:postgresql://localhost:5433/lf8_lets_meet_db";
    private static final String PG_USER = "user";
    private static final String PG_PASSWORD = "secret";

    // XML-Konstanten
    private static final String XML_FILE_PATH = "Lets_Meet_Hobbies.xml";

    public static void main(String[] args) {
        System.out.println("Starte XML-Datenmigration...");

        try (Connection pgConnection = DriverManager.getConnection(PG_URL, PG_USER, PG_PASSWORD)) {
            pgConnection.setAutoCommit(false); // Transaktionen aktivieren
            migrate(pgConnection);
            pgConnection.commit(); // Transaktion abschließen
            System.out.println("XML-Datenmigration erfolgreich abgeschlossen.");
        } catch (SQLException e) {
            System.err.println("Datenbankfehler während der XML-Migration: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("XML-Migration fehlgeschlagen: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void migrate(Connection pgConnection) throws Exception {
        File file = new File(XML_FILE_PATH);
        if (!file.exists()) {
            throw new java.io.FileNotFoundException("XML-Datei nicht gefunden: " + file.getAbsolutePath());
        }

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document xmlDoc = builder.parse(file);
        xmlDoc.getDocumentElement().normalize();

        NodeList userNodes = xmlDoc.getElementsByTagName("user");
        int importedCount = 0;
        int hobbyCount = 0;

        String userSql = "INSERT INTO app_user (user_email, first_name, last_name) VALUES (?, ?, ?) " +
                "ON CONFLICT (user_email) DO UPDATE SET " +
                "first_name = COALESCE(EXCLUDED.first_name, app_user.first_name), " +
                "last_name = COALESCE(EXCLUDED.last_name, app_user.last_name)";

        String hobbySql = "INSERT INTO hobby (name) VALUES (?) ON CONFLICT (name) DO NOTHING";
        String userHobbySql = "INSERT INTO user_hobby (user_email, hobby_id) VALUES (?, (SELECT hobby_id FROM hobby WHERE name = ?)) ON CONFLICT (user_email, hobby_id) DO NOTHING";

        try (PreparedStatement userStmt = pgConnection.prepareStatement(userSql);
             PreparedStatement hobbyStmt = pgConnection.prepareStatement(hobbySql);
             PreparedStatement userHobbyStmt = pgConnection.prepareStatement(userHobbySql)) {

            for (int i = 0; i < userNodes.getLength(); i++) {
                Element userElement = (Element) userNodes.item(i);
                String email = userElement.getElementsByTagName("email").item(0).getTextContent();
                String name = userElement.getElementsByTagName("name").item(0).getTextContent();

                String firstName = null;
                String lastName = null;
                Pattern pattern = Pattern.compile("([^,]+), (.+)");
                Matcher matcher = pattern.matcher(name);
                if (matcher.find()) {
                    lastName = matcher.group(1).trim();
                    firstName = matcher.group(2).trim();
                }

                userStmt.setString(1, email);
                userStmt.setString(2, firstName);
                userStmt.setString(3, lastName);
                userStmt.executeUpdate();
                importedCount++;

                NodeList hobbies = ((Element) userElement.getElementsByTagName("hobbies").item(0)).getElementsByTagName("hobby");
                for (int j = 0; j < hobbies.getLength(); j++) {
                    String hobbyName = hobbies.item(j).getTextContent();

                    hobbyStmt.setString(1, hobbyName);
                    hobbyStmt.executeUpdate();

                    userHobbyStmt.setString(1, email);
                    userHobbyStmt.setString(2, hobbyName);
                    userHobbyStmt.executeUpdate();
                    hobbyCount++;
                }
            }
        }
        System.out.printf("XML-Import: %d Benutzer und %d Hobbys erfolgreich migriert.%n", importedCount, hobbyCount);
    }
}
