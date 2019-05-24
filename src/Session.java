import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.Scanner;

public class Session {
    private final int port;
    private final String ip, user, password, dbName;
    private static int farmerCount, marketCount, producesCount, productsCount,  registersCount = 0;

    Connection connection;

    public Session(String ip, int port, String dbName, String user, String password) {
        this.ip = ip;
        this.port = port;
        this.dbName = dbName;
        this.user = user;
        this.password = password;

        connectToServer();
    }

    private void connectToServer() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String address = "jdbc:mysql://" + ip + ":" + port + "/" + dbName;
            connection = DriverManager.getConnection(address, user, password);
        } catch (Exception e) {
            System.out.println("There was a problem while connecting to server :(");
            e.printStackTrace();
        }
    }

    public void printColumnInTable(String table, String column) throws SQLException {
        Statement statement = this.connection.createStatement();
        String query = "SELECT " + column + " FROM " + table + " " + table.charAt(0);
        ResultSet resultSet = statement.executeQuery(query);

        while (resultSet.next())
            System.out.println(resultSet.getString(1)); // get the only column that exists
    }


    public void insertFarmersFromFile(File data) {
        try {
            Scanner scanner = new Scanner(data);
            Statement statement = this.connection.createStatement();

            if (scanner.hasNext())  // skip the column headers
                scanner.next();

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                int nextId = Session.farmerCount + 1;

                if (!line.equals("")) {
                    String[] entries = line.split(";");
                    String name = entries[0];
                    String lastName = entries[1];
                    String address = entries[2];
                    String zipcode = entries[3];
                    String city = entries[4];
                    String[] phones, emails;

                    if (entries[5].contains("|")) {
                        phones = new String[entries[5].split("\\|").length];
                        phones = entries[5].split("\\|");
                    } else {
                        phones = new String[1];
                        phones[0] = entries[5];
                    }

                    if (entries[6].contains("|")) {
                        emails = new String[entries[6].split("\\|").length];
                        emails = entries[6].split("\\|");
                    } else {
                        emails = new String[1];
                        emails[0] = entries[6];
                    }

                    String sql = "INSERT INTO Farmer "
                            + "VALUES " + "(" + nextId + ", " + "'"  + name +
                            "'" + "," + "'" + lastName + "'"+ ")";
                    statement.executeUpdate(sql);

                    for (String phone : phones) {
                        long phoneNumber = Long.valueOf(phone);
                        statement.executeUpdate("INSERT INTO FarmerPhoneNumber "
                                + "VALUES " + "(" + phoneNumber + "," + nextId + ")");
                    }

                    for (String email : emails) {
                        statement.executeUpdate("INSERT INTO FarmerEmail "
                                + "VALUES " + "(" + "'" + email + "'," + nextId + ")");
                    }

                    // Insert address, zipcode, city
                    statement.executeUpdate("INSERT INTO FarmerAddressZipcode "
                            + "VALUES " + "(" + nextId + "," + "'" + address + "'" + ","
                            + "'" + zipcode + "'" + ")");

                    statement.executeUpdate("INSERT INTO FarmerAddressCity "
                            + "VALUES " + "(" + nextId + "," + "'"
                            + city + "'" + "," + "'" + address + "'" + ")");

                }
                farmerCount += 1;
            }
        } catch (FileNotFoundException e) {
            System.out.println("File not found!");
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertTransactionsFromFile(File data) {

    }

    public void insertProductsFromFile(File data) {

    }

    public void insertMarketsFromFile(File data) {

    }

    public void insertRegistersFromFile(File data) {

    }
}
