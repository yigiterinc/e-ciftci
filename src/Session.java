import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.Scanner;

public class Session {
    private final int port;
    private final String ip, user, password, dbName;
    private int nextFarmerId, nextMarketId, nextProductId = 0;

    Connection connection;
    Statement statement;

    public Session(String ip, int port, String dbName, String user, String password)
            throws SQLException, ClassNotFoundException {

        this.ip = ip;
        this.port = port;
        this.dbName = dbName;
        this.user = user;
        this.password = password;

        connectToServer();
        getCurrentRowCounts();
    }

    private void connectToServer() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        String address = "jdbc:mysql://" + ip + ":" + port + "/" + dbName;
        connection = DriverManager.getConnection(address, user, password);
        statement = connection.createStatement();
    }

    private void getCurrentRowCounts() throws SQLException {
        nextFarmerId = getNumberOfRowsInTable("Farmer");
        nextMarketId = getNumberOfRowsInTable("Market");
        nextProductId = getNumberOfRowsInTable("Product");
    }

    private int getNumberOfRowsInTable(String table) throws SQLException {
        ResultSet rs = statement.executeQuery("SELECT COUNT(*) "
                + "FROM " + table);

        rs.next();
        int rowCount = rs.getInt(1);
        return rowCount;
    }

    public void insertFarmersFromFile(File data) throws SQLException, FileNotFoundException {
        Scanner scanner = new Scanner(data);

        if (scanner.hasNext())  // skip the column headers
            scanner.next();

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();

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

                // Execute the insert statements
                insertFarmer(name, lastName);
                insertFarmerPhoneNumber(phones);
                insertFarmerEmail(emails);
                insertFarmerAddressZipcode(address, zipcode);
                insertFarmerAddressCity(city, address);
            }
        }
    }


    private void insertFarmer(String name, String lastName) throws SQLException {
        this.nextFarmerId += 1;

        String sql = "INSERT INTO Farmer "
                + "VALUES " + "(" + this.nextFarmerId + ", " + "'" + name +
                "'" + "," + "'" + lastName + "'" + ")";
        statement.executeUpdate(sql);
    }

    private void insertFarmerPhoneNumber(String[] phones) throws SQLException {
        for (String phone : phones) {
            long phoneNumber = Long.valueOf(phone);
            statement.executeUpdate("INSERT INTO FarmerPhoneNumber "
                    + "VALUES " + "(" + phoneNumber + "," + this.nextFarmerId + ")");
        }
    }

    private void insertFarmerEmail(String[] emails) throws SQLException {
        for (String email : emails) {
            statement.executeUpdate("INSERT INTO FarmerEmail "
                    + "VALUES " + "(" + "'" + email + "'," + this.nextFarmerId + ")");
        }
    }

    private void insertFarmerAddressZipcode(String address, String zipcode) throws SQLException {
        statement.executeUpdate("INSERT INTO FarmerAddressZipcode "
                + "VALUES " + "(" + this.nextFarmerId + "," + "'" + address + "'" + ","
                + "'" + zipcode + "'" + ")");
    }


    private void insertFarmerAddressCity(String address, String city) throws SQLException {
        statement.executeUpdate("INSERT INTO FarmerAddressCity "
                + "VALUES " + "(" + this.nextFarmerId + "," + "'"
                + city + "'" + "," + "'" + address + "'" + ")");
    }

    public void insertTransactionsFromFile(File data) {

    }

    public void insertProductsFromFile(File data) throws SQLException, FileNotFoundException {
        Scanner scanner = new Scanner(data);

        if (scanner.hasNext())  // skip the column headers
            scanner.next();

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();

            if (!line.equals("")) {
                String[] entries = line.split(",");

                String plantName = entries[0];
                String plantDate = entries[1];
                String harvestDate = entries[2];
                int altitude = Integer.parseInt(entries[3]);
                int minTemperature = Integer.parseInt(entries[4]);
                String hardness = entries[5];

                insertProduct(plantName, Integer.parseInt(hardness));
                insertPlantDate_HarvestDate(plantDate, harvestDate);
                insertAltLevel_MinTemp(altitude, minTemperature);
            }
        }
    }

    private void insertProduct(String productName, int hardness) throws SQLException {
        nextProductId += 1;

        String sql = "INSERT INTO Product "
                + "VALUES " + "(" + this.nextProductId + ", " + "'"  + productName +
                "'" + "," + "'" + hardness + "'"+ ")";
        statement.executeUpdate(sql);
    }

    private void insertPlantDate_HarvestDate(String plantDate, String harvestDate) throws SQLException {
        statement.executeUpdate("INSERT INTO PlantDate_HarvestDate "
                + "VALUES " + "(" + this.nextProductId + "," + "'" + plantDate + "'" + ","
                + "'" + harvestDate + "'" + ")");
    }

    private void insertAltLevel_MinTemp(int altitude, int minTemperature) throws SQLException {
        statement.executeUpdate("INSERT INTO AltLevel_MinTemp "
                + "VALUES " + "(" + this.nextProductId + "," + "'"
                + altitude + "'" + "," + "'" + minTemperature + "'" + ")");
    }

    public void insertMarketsFromFile(File data) throws SQLException, FileNotFoundException {
        Scanner scanner = new Scanner(data);

        if (scanner.hasNext())  // skip the column headers
            scanner.next();

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();

            if (!line.equals("")) {
                String[] entries = line.split(";");

                String marketName = entries[0];
                String marketAddress = entries[1];
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

                insertMarket(marketName, marketAddress, budget);
                insertMarketAddressZipcode(marketAddress, zipcode);
                insertMarketAddressCity(marketAddress, city);
            }
        }
    }

    private void insertMarket(String marketName, String address, int budget) throws SQLException {
        this.nextMarketId += 1;
        String sql = "INSERT INTO Market "
                + "VALUES " + "(" + this.nextMarketId + ", " + "'" + marketName +
                "'" + "," + "'" + address + "'" + "," + budget + ")";
        statement.executeUpdate(sql);
    }

    private void insertMarketAddressZipcode(String marketAddress, String zipcode) throws SQLException {
        statement.executeUpdate("INSERT INTO MarketAddressZipcode "
                + "VALUES " + "(" + this.nextMarketId + "," + "'" + marketAddress + "'" + ","
                + "'" + zipcode + "'" + ")");
    }

    private void insertMarketAddressCity(String city, String marketAddress) throws SQLException {
        statement.executeUpdate("INSERT INTO MarketAddressCity "
                + "VALUES " + "(" + this.nextMarketId + "," + "'"
                + city + "'" + "," + "'" + marketAddress + "'" + ")");
    }

    public void insertRegistersFromFile(File data) {

    }

    public void insertProducesFromFile(File data) {

    }

    public void showTables() throws SQLException {
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
    }
}
