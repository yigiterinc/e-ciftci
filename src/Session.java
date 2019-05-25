import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.Scanner;

public class Session {
    private final int port;
    private final String ip, user, password, dbName;
    private int nextFarmerId, nextMarketId, nextProductId,nextTransactionId = 0;

    private Connection connection;
    private Statement statement;

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
        nextTransactionId = getNumberOfRowsInTable("Transaction");
    }

    private int getNumberOfRowsInTable(String table) throws SQLException {
        String sql = "SELECT COUNT(*) "
                   + "FROM " + table;
        ResultSet resultSet = statement.executeQuery(sql);

        resultSet.next();
        return resultSet.getInt(1);

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
                   + "VALUES " + convertToMysqlValuesForm(nextFarmerId, name, lastName);
        statement.executeUpdate(sql);
    }

    private void insertFarmerPhoneNumber(String[] phones) throws SQLException {
        for (String phone : phones) {
            long phoneNumber = Long.valueOf(phone);

            String sql = "INSERT INTO FarmerPhoneNumber "
                       + "VALUES " + convertToMysqlValuesForm(phoneNumber, nextFarmerId);
            statement.executeUpdate(sql);
        }
    }

    private void insertFarmerEmail(String[] emails) throws SQLException {
        for (String email : emails) {
            String sql = "INSERT INTO FarmerEmail "
                       + "VALUES " + convertToMysqlValuesForm(email, this.nextFarmerId);
            statement.executeUpdate(sql);
        }
    }

    private void insertFarmerAddressZipcode(String address, String zipcode) throws SQLException {
        String sql = "INSERT INTO FarmerAddressZipcode "
                   + "VALUES " + convertToMysqlValuesForm(this.nextFarmerId, address, zipcode);
        statement.executeUpdate(sql);
    }


    private void insertFarmerAddressCity(String address, String city) throws SQLException {
        String sql = "INSERT INTO FarmerAddressCity "
                   + "VALUES " + convertToMysqlValuesForm(this.nextFarmerId, city, address);
        statement.executeUpdate(sql);
    }

    public void insertTransactionsFromFile(File data) throws FileNotFoundException, SQLException {
        Scanner scanner = new Scanner(data);

        if (scanner.hasNext())  // skip the column headers
            scanner.next();

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();

            if (!line.equals("")) {
                String[] entries = line.split(";");
                String farmerName = entries[0];
                String farmerLastName = entries[1];
                String productName = entries[2];
                String marketName = entries[3];
                String marketAddress = entries[4];
                int amount = Integer.parseInt(entries[5]);
                String cardNumber = entries[6];

                int farmerId = getFarmerIdWithNameLastNameInRegisters(farmerName, farmerLastName);
                int productId = getProductIdWithProductNameInRegisters(productName);
                int marketId = getMarketIdWithMarketNameAddress(marketName, marketAddress);

                insertTransaction(productId, marketId, farmerId, amount, cardNumber);
            }
        }
    }

    private int getFarmerIdWithNameLastNameInRegisters(String farmerName, String farmerLastName) throws SQLException {
        String sql = "SELECT Farmer.f_id "
                   + "FROM Farmer, Registers "
                   + "WHERE Farmer.f_name = " + stringify(farmerName)
                   + " AND Farmer.f_last_name = " + stringify(farmerLastName)
                   + " AND Farmer.f_id = Registers.f_id";
        ResultSet resultSet = this.statement.executeQuery(sql);

        resultSet.next();
        return resultSet.getInt(1);
    }

    private int getProductIdWithProductNameInRegisters(String productName) throws SQLException {
        String sql = "SELECT Product.p_id"
                   + " FROM Product, Registers"
                   + " WHERE Product.p_name = " + stringify(productName)
                   + " AND Product.p_id = Registers.p_id";
        ResultSet resultSet = this.statement.executeQuery(sql);

        resultSet.next();
        return resultSet.getInt(1);
    }

    private int getMarketIdWithMarketNameAddress(String marketName, String marketAddress) throws SQLException {
        String sql = "SELECT Market.m_id "
                   + "FROM Market "
                   + "WHERE Market.m_name = " + stringify(marketName)
                   + " AND Market.address = " + stringify(marketAddress);
        ResultSet resultSet = this.statement.executeQuery(sql);

        resultSet.next();
        return resultSet.getInt(1);
    }

    private void insertTransaction(int productId, int marketId, int farmerId,
                                   int amount, String cardNumber) throws SQLException {
        this.nextTransactionId += 1;
        String sql = "INSERT INTO Transaction "
                   + "VALUES " + convertToMysqlValuesForm(this.nextTransactionId, productId,
                                                          marketId, farmerId, amount, cardNumber);
        statement.executeUpdate(sql);
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
                   + "VALUES " + convertToMysqlValuesForm(this.nextProductId, productName, hardness);
        statement.executeUpdate(sql);
    }

    private void insertPlantDate_HarvestDate(String plantDate, String harvestDate) throws SQLException {
        String sql = "INSERT INTO PlantDate_HarvestDate "
                   + "VALUES " + convertToMysqlValuesForm(this.nextProductId, plantDate, harvestDate);
        statement.executeUpdate(sql);
    }

    private void insertAltLevel_MinTemp(int altitude, int minTemperature) throws SQLException {
        String sql = "INSERT INTO AltLevel_MinTemp "
                   + "VALUES " + convertToMysqlValuesForm(this.nextProductId, altitude, minTemperature);
        statement.executeUpdate(sql);
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
                   + "VALUES " + convertToMysqlValuesForm(this.nextMarketId, marketName, address, budget);
        statement.executeUpdate(sql);
    }

    private void insertMarketAddressZipcode(String marketAddress, String zipcode) throws SQLException {
        String sql = "INSERT INTO MarketAddressZipcode "
                   + "VALUES " + convertToMysqlValuesForm(this.nextMarketId, marketAddress, zipcode);
        statement.executeUpdate(sql);
    }

    private void insertMarketAddressCity(String city, String marketAddress) throws SQLException {
        String sql = "INSERT INTO MarketAddressCity "
                   + "VALUES " + convertToMysqlValuesForm(this.nextMarketId, city, marketAddress);
        statement.executeUpdate(sql);
    }

    public void insertRegistersFromFile(File data) throws FileNotFoundException, SQLException {
        Scanner scanner = new Scanner(data);

        if (scanner.hasNext())  // skip the column headers
            scanner.next();

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();

            if (!line.equals("")) {
                String[] entries = line.split(";");

                String farmerName = entries[0];
                String farmerLastName = entries[1];
                String productName = entries[2];
                int quantity = Integer.parseInt(entries[3]);
                double price = Double.parseDouble(entries[4].replaceAll(",", "."));
                String iban = entries[5];

                // Get the farmer id with name and last name
                int farmerId = getFarmerIdWithNameLastName(farmerName, farmerLastName);
                // Get the pid with pname
                int productId = getProductIdWithProductName(productName);
                // Insert into registers(f_id, p_id, p_name, iban, price)
                insertRegister(farmerId, productId, quantity, iban, price);
            }
        }
    }

    private int getFarmerIdWithNameLastName(String name, String lastName) throws SQLException {
        String sql = "SELECT F.f_id "
                   + "FROM Farmer F "
                   + "WHERE F.f_name = " + stringify(name)
                   + " AND F.f_last_name = " + stringify(lastName);
        ResultSet resultSet = statement.executeQuery(sql);

        resultSet.next();
        return resultSet.getInt(1);
    }

    private int getProductIdWithProductName(String productName) throws SQLException {
        ResultSet resultSet = statement.executeQuery( "SELECT P.p_id "
                                                        + "FROM Product P "
                                                        + "WHERE P.p_name = "
                                                        + stringify(productName));

        resultSet.next();
        return resultSet.getInt(1);
    }

    private void insertRegister(int farmerId, int productId, int quantity,
                                String iban, double price) throws SQLException {

        String sql = "INSERT INTO Registers VALUES "
                   + convertToMysqlValuesForm(farmerId, productId, quantity, iban, price);
        statement.executeUpdate(sql);
    }

    // Returns the string variable in Mysql string form, x -> 'x'
    private String stringify(String toDB) {
        return "'" + toDB + "'";
    }

    // Gets a series of values, returns string for Mysql VALUES: (x, String y, z) -> (x, 'y', z)
    @SafeVarargs
    private <T> String convertToMysqlValuesForm(T... values) {
        String paramForm = "(";
        StringBuilder strBuilderParamForm = new StringBuilder(paramForm);

        for (int i = 0; i < values.length; i++) {
            T param = values[i];
            StringBuilder strBuilderAdd = new StringBuilder();

            if (param instanceof String) {
                strBuilderAdd.append(stringify(param.toString()));
            } else {
                strBuilderParamForm.append(param.toString());
            }

            strBuilderParamForm.append(strBuilderAdd.toString());

            if (i < values.length - 1)
                strBuilderParamForm.append(",");
        }

        strBuilderParamForm.append(')');

        return strBuilderParamForm.toString();
    }

    public void insertProducesFromFile(File data) throws FileNotFoundException, SQLException {
        Scanner scanner = new Scanner(data);

        if (scanner.hasNext())  // skip the column headers
            scanner.next();

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();

            if (!line.equals("")) {
                String[] entries = line.split(";");
                String farmerName = entries[0];
                String farmerLastName = entries[1];
                String productName = entries[2];
                int quantity = Integer.parseInt(entries[3]);
                int year = Integer.parseInt(entries[4]);

                int farmerId = getFarmerIdWithNameLastName(farmerName, farmerLastName);
                int productId = getProductIdWithProductName(productName);

                insertProduces(farmerId, productId, quantity, year);
            }
        }
    }


    private void insertProduces(int farmerId, int productId, int quantity, int year) throws SQLException {
        String sql = "INSERT INTO Produces "
                   + "VALUES " + convertToMysqlValuesForm(farmerId, productId, quantity, year);
        statement.executeUpdate(sql);
    }

    public void showTables() throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        ResultSet resultSet = meta.getTables(null, null, null, new String[]{"TABLE"});
        int rowCount = 0;

        System.out.println("+-----------------------+");
        System.out.println("| Tables_in_eciftci        |");
        System.out.println("+-----------------------+");

        while (resultSet.next()) {
            rowCount++;
            String tableName = resultSet.getString("TABLE_NAME");
            System.out.print("| " + tableName);
            for (int i = 0; i < 25 - tableName.length(); i++)
                System.out.print(" ");

            System.out.println("|");
        }

        System.out.println("+-----------------------+");
        System.out.println(rowCount + " rows in set ");
    }
}
