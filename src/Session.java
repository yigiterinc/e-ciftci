import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.Scanner;

public class Session {
    private final int port;
    private final String ip, user, password, dbName;
    private int farmerCount, marketCount, producesCount, productsCount, registersCount = 0;

    Connection connection;

    public Session(String ip, int port, String dbName, String user, String password) {
        this.ip = ip;
        this.port = port;
        this.dbName = dbName;
        this.user = user;
        this.password = password;

        connectToServer();
        getCurrentRowCounts();
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

    private void getCurrentRowCounts() {
        farmerCount = getNumberOfRowsInTable("Farmer");
        marketCount = getNumberOfRowsInTable("Market");
        producesCount = getNumberOfRowsInTable("Produces");
        productsCount = getNumberOfRowsInTable("Product");
        registersCount = getNumberOfRowsInTable("Registers");
    }

    private int getNumberOfRowsInTable(String table) {
        try {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("SELECT COUNT(*) "
                    + "FROM " + table);

            rs.next();
            int rowCount = rs.getInt(1);
            return rowCount;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return 0;
    }

    public void insertFarmersFromFile(File data) {
        try {
            Scanner scanner = new Scanner(data);
            Statement statement = this.connection.createStatement();

            if (scanner.hasNext())  // skip the column headers
                scanner.next();

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                farmerCount += 1;
                int nextId = farmerCount;

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
                        emails = entries[6].split("\\|");
                    } else {
                        emails = new String[1];
                        emails[0] = entries[6];
                    }

                    String sql = "INSERT INTO Farmer "
                            + "VALUES " + "(" + nextId + ", " + "'" + name +
                            "'" + "," + "'" + lastName + "'" + ")";
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

        try {
            Scanner scanner = new Scanner(data);
            Statement statement = this.connection.createStatement();

            if (scanner.hasNext())  // skip the column headers
                scanner.next();

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                productsCount += 1;
                int nextId = productsCount;
                
                if (!line.equals("")) {
                    String[] entries = line.split(",");
                    String pname = entries[0];
                    String pdate = entries[1];
                    String hdate = entries[2];
                    String alt = entries[3];
                    String mintemp = entries[4];
                    String hardness = entries[5];

                    String sql = "INSERT INTO Product "
                            + "VALUES " + "(" + nextId + ", " + "'"  + pname +
                            "'" + "," + "'" + hardness + "'"+ ")";
                    statement.executeUpdate(sql);

                    // Insert address, zipcode, city
                    statement.executeUpdate("INSERT INTO PlantDate_HarvestDate "
                            + "VALUES " + "(" + nextId + "," + "'" + pdate + "'" + ","
                            + "'" + hdate + "'" + ")");

                    statement.executeUpdate("INSERT INTO AltLevel_MinTemp "
                            + "VALUES " + "(" + nextId + "," + "'"
                            + alt + "'" + "," + "'" + mintemp + "'" + ")");

                }
                productsCount += 1;
            }
        } catch (FileNotFoundException e) {
            System.out.println("File not found!");
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void insertMarketsFromFile(File data) {
        try {
            Scanner scanner = new Scanner(data);
            Statement statement = this.connection.createStatement();

            if (scanner.hasNext())  // skip the column headers
                scanner.next();

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                marketCount += 1;
                int nextId = marketCount;

                if (!line.equals("")) {
                    String[] entries = line.split(";");
                    String name = entries[0];
                    String address = entries[1];
                    String zipcode = entries[2];
                    String city = entries[3];
                    String[] phones;
                    int budget = Integer.parseInt(entries[5]);

                    if (entries[4].contains("|")) {
                        phones = new String[entries[5].split("\\|").length];
                        phones = entries[4].split("\\|");
                    } else {
                        phones = new String[1];
                        phones[0] = entries[4];
                    }

                    String sql = "INSERT INTO Market "
                            + "VALUES " + "(" + nextId + ", " + "'" + name +
                            "'" + "," + "'" + address + "'" + "," + budget + ")";
                    statement.executeUpdate(sql);

                    // Insert address, city
                    statement.executeUpdate("INSERT INTO MarketAddressZipcode "
                            + "VALUES " + "(" + nextId + "," + "'" + address + "'" + ","
                            + "'" + zipcode + "'" + ")");

                    statement.executeUpdate("INSERT INTO MarketAddressCity "
                            + "VALUES " + "(" + nextId + "," + "'"
                            + city + "'" + "," + "'" + address + "'" + ")");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void insertRegistersFromFile(File data) {

    }

    public void insertProducesFromFile(File data) {

    }

    public void showTables() {
        try {
            ResultSet rs = null;
            DatabaseMetaData meta = connection.getMetaData();
            rs = meta.getTables(null, null, null, new String[]{"TABLE"});
            int rowCount = 0;

            System.out.println("+-----------------------+");
            System.out.println("| Tables_in_eciftci        |");
            System.out.println("+-----------------------+");

            while (rs.next()) {
                rowCount++;
                String tableName = rs.getString("TABLE_NAME");
                System.out.print("| " + tableName);
                for (int i = 0; i < 25 - tableName.length(); i++)
                    System.out.print(" ");

                System.out.println("|");
            }
            System.out.println("+-----------------------+");
            System.out.println(rowCount + " rows in set ");
        } catch (Exception e) {
        }
    }
}
