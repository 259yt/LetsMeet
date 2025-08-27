DROP TABLE IF EXISTS address;
DROP TABLE IF EXISTS photo;
DROP TABLE IF EXISTS interest;
DROP TABLE IF EXISTS friendship;
DROP TABLE IF EXISTS gender_interest;
DROP TABLE IF EXISTS gender;
DROP TABLE IF EXISTS message;
DROP TABLE IF EXISTS conversation;
DROP TABLE IF EXISTS user_hobby;
DROP TABLE IF EXISTS hobby;
DROP TABLE IF EXISTS phone_number;
DROP TABLE IF EXISTS app_user;

-- Tabelle für die Hauptbenutzerinformationen

CREATE TABLE app_user
(
    user_email    VARCHAR(255) PRIMARY KEY,
    first_name    VARCHAR(255),
    last_name     VARCHAR(255),
    gender        VARCHAR(20),
    date_of_birth DATE
);

---

-- Tabelle für Adressinformationen
CREATE TABLE address
(
    address_id   INT PRIMARY KEY,
    user_email   VARCHAR(255) REFERENCES app_user (user_email),
    street       VARCHAR(255),
    house_number VARCHAR(50),
    city         VARCHAR(255),
    postal_code  VARCHAR(10)
);

---

-- Tabelle für Telefonnummern
CREATE TABLE phone_number
(
    phone_number VARCHAR(50) PRIMARY KEY,
    user_email   VARCHAR(255) REFERENCES app_user (user_email)
);

---

-- Tabelle für Fotos
CREATE TABLE photo
(
    photo_id           INT PRIMARY KEY,
    user_email         VARCHAR(255) REFERENCES app_user (user_email),
    date               DATE,
    is_profile_picture BOOLEAN
);

---

-- Tabelle für Hobbys
CREATE TABLE hobby
(
    hobby_id INT PRIMARY KEY,
    name     VARCHAR(255)
);

---

-- Tabelle für Geschlechter
CREATE TABLE gender
(
    gender_id   INT PRIMARY KEY,
    gender_name VARCHAR(50)
);

---

-- Tabelle für Konversationen
CREATE TABLE conversation
(
    conversation_id  INT PRIMARY KEY,
    date_of_creation DATE
);

---

-- Zwischentabelle zur Verknüpfung von Benutzern und Hobbys
CREATE TABLE user_hobby
(
    user_email VARCHAR(255) REFERENCES app_user (user_email),
    hobby_id   INT REFERENCES hobby (hobby_id),
    priority   INT,
    PRIMARY KEY (user_email, hobby_id)
);

---

-- Zwischentabelle für die Freundschaftsbeziehung zwischen zwei Benutzern
CREATE TABLE friendship
(
    friendship_id    INT PRIMARY KEY,
    user_id_1        VARCHAR(255) REFERENCES app_user (user_email),
    user_id_2        VARCHAR(255) REFERENCES app_user (user_email),
    status           VARCHAR(50),
    date_of_change   DATE,
    date_of_creation DATE
);

---

-- Zwischentabelle für Interessen (Likes)
CREATE TABLE interest
(
    interest_id      INT PRIMARY KEY,
    from_user_id     VARCHAR(255) REFERENCES app_user (user_email),
    to_user_id       VARCHAR(255) REFERENCES app_user (user_email),
    date_of_change   DATE,
    date_of_creation DATE
);

---

-- Zwischentabelle für die Geschlechtsinteressen
CREATE TABLE gender_interest
(
    user_email VARCHAR(255) REFERENCES app_user (user_email),
    gender_id  INT REFERENCES gender (gender_id),
    PRIMARY KEY (user_email, gender_id)
);

---

-- Tabelle für Nachrichten
CREATE TABLE message
(
    message_id      INT PRIMARY KEY,
    conversation_id INT REFERENCES conversation (conversation_id),
    sender_id       VARCHAR(255) REFERENCES app_user (user_email),
    receiver_id     VARCHAR(255) REFERENCES app_user (user_email),
    text            TEXT,
    date_of_sending DATE
);