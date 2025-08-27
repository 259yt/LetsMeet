import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class mongo_db {

    public static void main(String[] args) {
        try {
            // Verbindung zu MongoDB herstellen
            MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
            MongoDatabase database = mongoClient.getDatabase("lets-meet-db");
            System.out.println("Verbindung zur Datenbank 'lets-meet-db' hergestellt.");

            // Collections leeren, falls sie bereits existieren
            database.getCollection("users").drop();
            database.getCollection("hobbies").drop();

            // Daten importieren
            importUserData(database);
            importHobbyData(database);

            mongoClient.close();
            System.out.println("Datenimport abgeschlossen und Verbindung geschlossen.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void importUserData(MongoDatabase database) throws Exception {
        System.out.println("Importiere Benutzerdaten...");
        MongoCollection<Document> collection = database.getCollection("users");

        File xmlFile = new File("users.xml");
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        org.w3c.dom.Document doc = dBuilder.parse(xmlFile);
        doc.getDocumentElement().normalize();

        NodeList userNodes = doc.getElementsByTagName("user");
        List<Document> userDocuments = new ArrayList<>();

        for (int i = 0; i < userNodes.getLength(); i++) {
            Element userElement = (Element) userNodes.item(i);
            Document userDoc = new Document();
            userDoc.put("email", userElement.getAttribute("email"));
            userDoc.put("first_name", userElement.getAttribute("first_name"));
            userDoc.put("last_name", userElement.getAttribute("last_name"));
            userDoc.put("gender", userElement.getAttribute("gender"));
            userDoc.put("date_of_birth", userElement.getAttribute("date_of_birth"));

            // Hier könnten Hobbys, Adressen usw. direkt als verschachtelte Dokumente hinzugefügt werden
            // Das zeigt die Flexibilität von MongoDB

            userDocuments.add(userDoc);
        }
        collection.insertMany(userDocuments);
        System.out.println("Benutzer erfolgreich importiert.");
    }

    private static void importHobbyData(MongoDatabase database) throws Exception {
        System.out.println("Importiere Hobby-Daten...");
        MongoCollection<Document> collection = database.getCollection("hobbies");

        File xmlFile = new File("hobbies.xml");
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        org.w3c.dom.Document doc = dBuilder.parse(xmlFile);
        doc.getDocumentElement().normalize();

        NodeList hobbyNodes = doc.getElementsByTagName("Hobby");
        List<Document> hobbyDocuments = new ArrayList<>();

        for (int i = 0; i < hobbyNodes.getLength(); i++) {
            Element hobbyElement = (Element) hobbyNodes.item(i);
            Document hobbyDoc = new Document();
            hobbyDoc.put("id", Integer.parseInt(hobbyElement.getAttribute("ID")));
            hobbyDoc.put("name", hobbyElement.getAttribute("Name"));
            hobbyDoc.put("priority", Integer.parseInt(hobbyElement.getAttribute("Priority")));

            hobbyDocuments.add(hobbyDoc);
        }
        collection.insertMany(hobbyDocuments);
        System.out.println("Hobbys erfolgreich importiert.");
    }
}