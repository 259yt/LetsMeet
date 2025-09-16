package com.letsmeet;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

            // Zuerst alle Tabellen leeren, um Fremdschlüsselkonflikte zu vermeiden
            pgConnection.prepareStatement("DELETE FROM message").executeUpdate();
            pgConnection.prepareStatement("DELETE FROM conversation").executeUpdate();
            pgConnection.prepareStatement("DELETE FROM interest").executeUpdate();
            pgConnection.prepareStatement("DELETE FROM friendship").executeUpdate();
            pgConnection.prepareStatement("DELETE FROM phone_number").executeUpdate();
            pgConnection.prepareStatement("DELETE FROM user_hobby").executeUpdate();
            pgConnection.prepareStatement("DELETE FROM app_user").executeUpdate();

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
            // Zuerst alle abhängigen Tabellen leeren, um Fremdschlüsselkonflikte zu vermeiden
            pgConnection.prepareStatement("DELETE FROM phone_number").executeUpdate();
            pgConnection.prepareStatement("DELETE FROM user_hobby").executeUpdate();
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

                // Extract gender if available
                String gender = null;
                if (userDoc.containsKey("gender")) {
                    gender = userDoc.getString("gender");
                }

                // Extract date of birth if available
                java.sql.Date dateOfBirth = null;
                if (userDoc.containsKey("date_of_birth")) {
                    String dobString = userDoc.getString("date_of_birth");
                    if (dobString != null && !dobString.isEmpty()) {
                        try {
                            // Parse the date (assuming format like "1990-01-01")
                            dateOfBirth = java.sql.Date.valueOf(dobString);
                        } catch (IllegalArgumentException e) {
                            System.err.println("Invalid date format for " + email + ": " + dobString);
                        }
                    }
                }

                userStmt.setString(1, email);
                userStmt.setString(2, firstName);
                userStmt.setString(3, lastName);
                userStmt.setString(4, gender);  // Set gender (can be null)
                userStmt.setDate(5, dateOfBirth);  // Set date of birth (can be null)
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

        // First, get the next available ID for the interest table
        String getMaxIdSql = "SELECT MAX(interest_id) FROM interest";
        int maxId = 0;
        try (PreparedStatement maxIdStmt = pgConnection.prepareStatement(getMaxIdSql);
             ResultSet rs = maxIdStmt.executeQuery()) {
            if (rs.next() && rs.getObject(1) != null) {
                maxId = rs.getInt(1);
            }
        }

        String interestsSql = "INSERT INTO interest (interest_id, \"from_user_id\", \"to_user_id\", \"date_of_creation\") VALUES (?, ?, ?, NOW())";

        try (PreparedStatement interestsStmt = pgConnection.prepareStatement(interestsSql)) {
            pgConnection.prepareStatement("DELETE FROM interest").executeUpdate();

            for (Document userDoc : usersCollection.find()) {
                String userEmail = userDoc.getString("_id");
                List<Document> likes = userDoc.getList("likes", Document.class);

                if (likes != null) {
                    for (Document likeDoc : likes) {
                        maxId++; // Increment for each new interest
                        interestsStmt.setInt(1, maxId);
                        interestsStmt.setString(2, userEmail);
                        interestsStmt.setString(3, likeDoc.getString("liked_email"));
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

        // First, get the next available ID for the message table
        String getMaxIdSql = "SELECT MAX(message_id) FROM message";
        int maxId = 0;
        try (PreparedStatement maxIdStmt = pgConnection.prepareStatement(getMaxIdSql);
             ResultSet rs = maxIdStmt.executeQuery()) {
            if (rs.next() && rs.getObject(1) != null) {
                maxId = rs.getInt(1);
            }
        }

        String messageSql = "INSERT INTO message (message_id, \"conversation_id\", \"sender_id\", \"receiver_id\", \"text\", \"date_of_sending\") VALUES (?, ?, ?, ?, ?, ?)";

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        try (PreparedStatement messageStmt = pgConnection.prepareStatement(messageSql)) {
            pgConnection.prepareStatement("DELETE FROM message").executeUpdate();

            for (Document userDoc : usersCollection.find()) {
                String senderEmail = userDoc.getString("_id");
                List<Document> messages = userDoc.getList("messages", Document.class);

                if (messages != null) {
                    for (Document messageDoc : messages) {
                        maxId++; // Increment for each new message
                        Integer conversationId = messageDoc.getInteger("conversation_id");
                        String receiverEmail = messageDoc.getString("receiver_email");
                        String text = messageDoc.getString("message");
                        String timestampString = messageDoc.getString("timestamp");

                        LocalDateTime messageDate = LocalDateTime.parse(timestampString, formatter);

                        messageStmt.setInt(1, maxId);
                        messageStmt.setInt(2, conversationId);
                        messageStmt.setString(3, senderEmail);
                        messageStmt.setString(4, receiverEmail);
                        messageStmt.setString(5, text);
                        messageStmt.setTimestamp(6, Timestamp.valueOf(messageDate));
                        messageStmt.executeUpdate();
                    }
                }
            }
            System.out.println("Nachrichten erfolgreich migriert.");
        }
    }
}
