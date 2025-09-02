import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class FullPostgresMigrator {

    // Ersetzen Sie dies mit Ihren tatsächlichen Werten
    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String MONGO_DB = "LetsMeet";

    private static final String PG_URL = "jdbc:postgresql://localhost:5433/lf8_lets_meet_db";
    private static final String PG_USER = "user";
    private static final String PG_PASSWORD = "secret";

    public static void main(String[] args) {
        try (MongoClient mongoClient = MongoClients.create(MONGO_URI);
             Connection pgConnection = DriverManager.getConnection(PG_URL, PG_USER, PG_PASSWORD)) {

            System.out.println("Verbindung zu MongoDB und PostgreSQL hergestellt.");

            // Migration der Hauptbenutzerdaten
            migrateUsers(mongoClient, pgConnection);

            // Migration der Beziehungen
            migrateFriendships(mongoClient, pgConnection);
            migrateInterests(mongoClient, pgConnection);

            // Zuerst Konversationen migrieren, dann Nachrichten
            migrateConversations(mongoClient, pgConnection);
            migrateMessages(mongoClient, pgConnection);

            System.out.println("Vollständige Migration abgeschlossen.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void migrateUsers(MongoClient mongoClient, Connection pgConnection) throws Exception {
        System.out.println("Migriere Benutzer...");
        MongoDatabase mongoDb = mongoClient.getDatabase(MONGO_DB);
        MongoCollection<Document> usersCollection = mongoDb.getCollection("users");

        String userSql = "INSERT INTO app_user (user_email, first_name, last_name, gender, date_of_birth) VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement userStmt = pgConnection.prepareStatement(userSql)) {
            pgConnection.prepareStatement("DELETE FROM app_user").executeUpdate();

            for (Document userDoc : usersCollection.find()) {
                String email = userDoc.getString("_id");
                String fullName = userDoc.getString("name");
                String firstName = null;
                String lastName = null;

                if (fullName != null && fullName.contains(", ")) {
                    String[] nameParts = fullName.split(", ", 2);
                    lastName = nameParts[0].trim();
                    if (nameParts.length > 1) {
                        firstName = nameParts[1].trim();
                    }
                }

                userStmt.setString(1, email);
                userStmt.setString(2, firstName);
                userStmt.setString(3, lastName);
                userStmt.setNull(4, java.sql.Types.VARCHAR);
                userStmt.setNull(5, java.sql.Types.DATE);
                userStmt.executeUpdate();
            }
            System.out.println("Benutzerdaten erfolgreich migriert.");
        }
    }

    private static void migrateFriendships(MongoClient mongoClient, Connection pgConnection) throws Exception {
        System.out.println("Migriere Freundschaften...");
        MongoDatabase mongoDb = mongoClient.getDatabase(MONGO_DB);
        MongoCollection<Document> usersCollection = mongoDb.getCollection("users");

        String friendshipSql = "INSERT INTO friendship (\"user_id_1\", \"user_id_2\") VALUES (?, ?)";

        try (PreparedStatement friendshipStmt = pgConnection.prepareStatement(friendshipSql)) {
            pgConnection.prepareStatement("DELETE FROM friendship").executeUpdate();

            for (Document userDoc : usersCollection.find()) {
                String userEmailFrom = userDoc.getString("_id");
                List<String> friends = userDoc.getList("friends", String.class);

                if (friends != null) {
                    for (String friendEmailTo : friends) {
                        friendshipStmt.setString(1, userEmailFrom);
                        friendshipStmt.setString(2, friendEmailTo);
                        friendshipStmt.executeUpdate();
                    }
                }
            }
            System.out.println("Freundschaften erfolgreich migriert.");
        }
    }

    private static void migrateInterests(MongoClient mongoClient, Connection pgConnection) throws Exception {
        System.out.println("Migriere Interessen...");
        MongoDatabase mongoDb = mongoClient.getDatabase(MONGO_DB);
        MongoCollection<Document> usersCollection = mongoDb.getCollection("users");

        String interestsSql = "INSERT INTO interest (\"from_user_id\", \"to_user_id\", \"date_of_creation\") VALUES (?, ?, NOW())";

        try (PreparedStatement interestsStmt = pgConnection.prepareStatement(interestsSql)) {
            pgConnection.prepareStatement("DELETE FROM interest").executeUpdate();

            for (Document userDoc : usersCollection.find()) {
                String userEmail = userDoc.getString("_id");
                List<Document> likes = userDoc.getList("likes", Document.class);

                if (likes != null) {
                    for (Document likeDoc : likes) {
                        interestsStmt.setString(1, userEmail);
                        interestsStmt.setString(2, likeDoc.getString("liked_email"));
                        interestsStmt.executeUpdate();
                    }
                }
            }
            System.out.println("Interessen erfolgreich migriert.");
        }
    }

    private static void migrateConversations(MongoClient mongoClient, Connection pgConnection) throws Exception {
        System.out.println("Sammle und migriere Konversationen...");
        MongoDatabase mongoDb = mongoClient.getDatabase(MONGO_DB);
        MongoCollection<Document> usersCollection = mongoDb.getCollection("users");

        Set<Integer> conversationIds = new HashSet<>();
        for (Document userDoc : usersCollection.find()) {
            List<Document> messages = userDoc.getList("messages", Document.class);
            if (messages != null) {
                for (Document messageDoc : messages) {
                    Integer conversationId = messageDoc.getInteger("conversation_id");
                    conversationIds.add(conversationId);
                }
            }
        }

        // Fügt die conversation_id manuell ein, um die Fremdschlüsselintegrität zu erhalten
        String conversationSql = "INSERT INTO conversation (conversation_id, date_of_creation) VALUES (?, NOW())";
        try (PreparedStatement conversationStmt = pgConnection.prepareStatement(conversationSql)) {
            pgConnection.prepareStatement("DELETE FROM conversation").executeUpdate();

            for (Integer convId : conversationIds) {
                conversationStmt.setInt(1, convId);
                conversationStmt.executeUpdate();
            }
        }
        System.out.println("Konversationen erfolgreich migriert.");
    }

    private static void migrateMessages(MongoClient mongoClient, Connection pgConnection) throws Exception {
        System.out.println("Migriere Nachrichten...");
        MongoDatabase mongoDb = mongoClient.getDatabase(MONGO_DB);
        MongoCollection<Document> usersCollection = mongoDb.getCollection("users");

        // message_id wird automatisch generiert, daher nicht im INSERT enthalten
        String messageSql = "INSERT INTO message (\"conversation_id\", \"sender_id\", \"receiver_id\", \"text\", \"date_of_sending\") VALUES (?, ?, ?, ?, ?)";

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        try (PreparedStatement messageStmt = pgConnection.prepareStatement(messageSql)) {
            pgConnection.prepareStatement("DELETE FROM message").executeUpdate();

            for (Document userDoc : usersCollection.find()) {
                String senderEmail = userDoc.getString("_id");
                List<Document> messages = userDoc.getList("messages", Document.class);

                if (messages != null) {
                    for (Document messageDoc : messages) {
                        Integer conversationId = messageDoc.getInteger("conversation_id");
                        String receiverEmail = messageDoc.getString("receiver_email");
                        String text = messageDoc.getString("message");
                        String timestampString = messageDoc.getString("timestamp");

                        LocalDateTime messageDate = LocalDateTime.parse(timestampString, formatter);

                        messageStmt.setInt(1, conversationId);
                        messageStmt.setString(2, senderEmail);
                        messageStmt.setString(3, receiverEmail);
                        messageStmt.setString(4, text);
                        messageStmt.setTimestamp(5, Timestamp.valueOf(messageDate));
                        messageStmt.executeUpdate();
                    }
                }
            }
            System.out.println("Nachrichten erfolgreich migriert.");
        }
    }
}