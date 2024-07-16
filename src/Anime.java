import java.sql.*;

public class Anime {
    private final static String PROTOCOL = "jdbc:postgresql://";
    private final static String DRIVER = "org.postgresql.Driver";
    private final static String URL_LOCALE_NAME = "localhost/";

    private final static String DATABASE_NAME = "fightEvilDB";

    private final static String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    private final static String USER_NAME = "postgres";
    private final static String DATABASE_PASSWORD = "postgres";

    public static void main(String[] args) {
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASSWORD)) {
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            getAllAnime(connection);
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            getSenen(connection, "senen");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            getAllAuthor(connection);
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            getAllStudio(connection);
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            deleteAnime(connection, "boruto", "masashi kishimoto");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            changeAnime(connection, "qwert", "Black");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            changeStudio(connection, 2005, "1-A");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            getAnimeByTaito(connection, "taito cuba");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            getAuthorsAnime(connection, "taito cuba");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            addNewAuthor(connection, "hiromu arakava", "female", 1973);
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    public static void checkDriver() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBS-драйвера!");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB() {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASSWORD);
        } catch (SQLException e) {
            System.out.println("Нет базы данных или неправильные данные пароля и логина");
            throw new RuntimeException(e);
        }
    }

    public static void getAllAnime(Connection connection) throws SQLException {
        String columnName0 = "name";
        String param0 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT name FROM anime");
        while (rs.next()) {
            param0 = rs.getString(columnName0);
            System.out.println(param0);
        }
    }

    public static void getSenen(Connection connection, String genre) throws SQLException {
        if (genre == null || genre.isBlank()) return;
        System.out.println(genre + ":");
        String columnName0 = "name";

        PreparedStatement statement = connection.prepareStatement("SELECT name " +
                "FROM anime " +
                "WHERE genre = ?");
        statement.setString(1, genre);
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(columnName0));
        }
    }

    public static void getAllAuthor(Connection connection) throws SQLException {
        String columnName0 = "full_author_name";
        String columnName1 = "gender";
        String columnName2 = "year_of_birth";


        String param0 = null;
        String param1 = null;
        int param2 = -1;


        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT full_author_name, gender, year_of_birth FROM author");

        while (rs.next()) {
            param0 = rs.getString(columnName0);
            param1 = rs.getString(columnName1);
            param2 = rs.getInt(columnName2);
            System.out.println(param0 + " " + param1 + " " + param2);
        }
    }

    public static void getAllStudio(Connection connection) throws SQLException {

        String columnName1 = "atelier_name";
        String columnName2 = "year_of_foundation";


        String param1 = null;
        int param2 = -1;


        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT atelier_name, year_of_foundation FROM atelier");

        while (rs.next()) {
            param1 = rs.getString(columnName1);
            param2 = rs.getInt(columnName2);
            System.out.println(param1 + "  |  " + param2);
        }
    }

    public static void deleteAnime(Connection connection, String name, String author) throws SQLException {
        if (name == null || name.isBlank() || author == null || author.isBlank()) return;
        PreparedStatement statement = connection.prepareStatement("DELETE FROM anime WHERE name = ? and author = ?");
        statement.setString(1, name);
        statement.setString(2, author);
        statement.executeUpdate();

        System.out.println("DELETE");
    }

    public static void changeAnime(Connection connection, String name, String studio) throws SQLException {
        if (name == null || studio == null || name.isBlank() || studio.isBlank())
            return;


        PreparedStatement statement = connection.prepareStatement("UPDATE anime SET name = ? WHERE studio = ?");
        statement.setString(1, name);
        statement.setString(2, studio);
        statement.executeUpdate();

        System.out.println("UPDATED ANIME");
    }


    public static void changeStudio(Connection connection, int year_of_foundation, String atelier_name) throws SQLException {
        if (year_of_foundation <= 0 || atelier_name == null || atelier_name.isBlank())
            return;

        PreparedStatement statement = connection.prepareStatement("UPDATE atelier SET atelier_name = ? WHERE year_of_foundation = ?");
        statement.setInt(2, year_of_foundation);
        statement.setString(1, atelier_name);
        statement.executeUpdate();

        System.out.println("atelier update");
    }

    public static void getAnimeByTaito(Connection connection, String author) throws SQLException {
        if (author == null || author.isBlank()) return;
        System.out.println(author + ":");
        String columnName0 = "name";

        PreparedStatement statement = connection.prepareStatement("SELECT name " +
                "FROM anime " +
                "WHERE author = ?");
        statement.setString(1, author);
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(columnName0));
        }
    }
    public static void getAuthorsAnime(Connection connection, String name) throws SQLException {
        if (name == null || name.isBlank()) return;
        name = '%' + name + '%';

        PreparedStatement statement = connection.prepareStatement(
                "SELECT author.full_author_name, anime.name, atelier.atelier_name " +
                        "FROM author " +
                        "JOIN anime ON anime.author = author.full_author_name " +
                        "JOIN atelier ON atelier.atelier_name = anime.studio " +
                        "WHERE author.full_author_name LIKE ?");
        statement.setString(1, name);
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getString(2) + " | " + rs.getString(3) );
        }

    }

    public static void addNewAuthor(Connection connection, String full_author_name, String gender, int year_of_birth) throws SQLException {
        if (full_author_name == null || full_author_name.isBlank() || gender == null || gender.isBlank() || year_of_birth <= 0)
            return;

        PreparedStatement statement = connection.prepareStatement("INSERT INTO author (full_author_name, gender, year_of_birth) VALUES (?, ?, ?)");
        statement.setString(1, full_author_name);
        statement.setString(2, gender);
        statement.setInt(3, year_of_birth);

        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getString(2) + " | " + rs.getInt(3) );
        }
    }


}