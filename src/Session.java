import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Scanner;

class Session {
    private final int PORT;
    private final String IP, USER, PASSWORD, DB_NAME;

    private static final String RESOURCES_PATH = "src/Resources/";
    private static final String[] RESOURCE_FILE_NAMES = {"farmers", "markets", "products",
                                                         "produces", "registers", "buys"};

    private int nextFarmerId, nextMarketId, nextProductId,nextTransactionId = 0;
    private boolean connectedToServer = false;

    private Connection connection;
    private Statement statement;

    Session(String ip, int port, String dbName, String USER, String PASSWORD) {
        this.IP = ip;
        this.PORT = port;
        this.DB_NAME = dbName;
        this.USER = USER;
        this.PASSWORD = PASSWORD;

        connectToServer();
        getCurrentRowCounts();
    }

    private void connectToServer() {
        int trialsLeft = 5; // # of trials to connect before quitting the application

        try {
            System.out.println("Opening a connection to " + IP + ":" + PORT + "/" + DB_NAME);
            Class.forName("com.mysql.cj.jdbc.Driver");
            String address = "jdbc:mysql://" + IP + ":" + PORT + "/" + DB_NAME;
            connection = DriverManager.getConnection(address, USER, PASSWORD);
            statement = connection.createStatement();
            connectedToServer = true;
            System.out.println("Connection successful");
        } catch (Exception e) {
            System.out.println("Connection to server failed! Trying again...");
            connectToServer(trialsLeft);
        }
    }

    private void connectToServer(int trialsLeft) {
        connectToServer();

        if (connectedToServer) {
            System.out.println("Connection successful");
            return;
        }

        if (trialsLeft > 0) {
            connectToServer(trialsLeft - 1);
        } else {
            System.out.println("Can't connect to server! Quitting...");
            System.exit(-1);
        }
    }

    private void getCurrentRowCounts() {
        try {
            nextFarmerId = getNumberOfRowsInTable("Farmer");
            nextMarketId = getNumberOfRowsInTable("Market");
            nextProductId = getNumberOfRowsInTable("Product");
            nextTransactionId = getNumberOfRowsInTable("Transaction");
        } catch (SQLException e) {
            System.out.println("Something went wrong while getting the current row counts");
            e.printStackTrace();
        }
    }

    private int getNumberOfRowsInTable(String table) throws SQLException {
        String sql = "SELECT COUNT(*) "
                   + "FROM " + table;
        ResultSet resultSet = statement.executeQuery(sql);

        resultSet.next();
        return resultSet.getInt(1);
    }

    void loadFromFiles() {
        File[] files = new File[6];

        for (int i = 0; i < RESOURCE_FILE_NAMES.length; i++)
            files[i] = new File(RESOURCES_PATH + RESOURCE_FILE_NAMES[i] + ".csv");

        try {
            loadFarmersFromFile(files[0]);
            loadMarketsFromFile(files[1]);
            loadProductsFromFile(files[2]);
            loadProducesFromFile(files[3]);
            loadRegistersFromFile(files[4]);
            loadTransactionsFromFile(files[5]);
            System.out.println("Data is successfully loaded");
        } catch (Exception e) {
            System.out.println("Failed to load the data");
            e.printStackTrace();
        }
    }

    void loadFarmersFromFile(File data) throws SQLException, FileNotFoundException {
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
                insertFarmerRelatedData(name, lastName, zipcode, city, phones, emails);
            }
        }
    }

    private void insertFarmer(String name, String lastName) throws SQLException {
        this.nextFarmerId += 1;

        String sql = "INSERT INTO Farmer "
                   + "VALUES " + convertToMysqlValuesForm(nextFarmerId, name, lastName);
        statement.executeUpdate(sql);
    }

    private void insertFarmerPhoneNumbers(String[] phones) throws SQLException {
        for (String phone : phones) {
            long phoneNumber = Long.valueOf(phone);

            String sql = "INSERT INTO FarmerPhoneNumber "
                       + "VALUES " + convertToMysqlValuesForm(phoneNumber, nextFarmerId);
            statement.executeUpdate(sql );
        }
    }

    private void insertFarmerEmails(String[] emails) throws SQLException {
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

    private void insertFarmerAddressCity(String city, String address) throws SQLException {
        String sql = "INSERT INTO FarmerAddressCity "
                   + "VALUES " + convertToMysqlValuesForm(this.nextFarmerId, city, address);
        statement.executeUpdate(sql);
    }

    void loadTransactionsFromFile(File data) throws FileNotFoundException, SQLException {
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

    void loadProductsFromFile(File data) throws SQLException, FileNotFoundException {
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

    void loadMarketsFromFile(File data) throws SQLException, FileNotFoundException {
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
                String[] phones = entries;
                int budget = Integer.parseInt(entries[5]);

                if (entries[4].contains("|")) {
                    phones = new String[entries[5].split("\\|").length];
                    phones = entries[4].split("\\|");
                } else {
                    phones = new String[1];
                    phones[0] = entries[4];
                }

                insertMarketRelatedData(marketName, marketAddress, zipcode, city, budget, phones);
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

    void loadRegistersFromFile(File data) throws FileNotFoundException, SQLException {
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
        String valuesFormed = "(";
        StringBuilder valuesFormedBuilder = new StringBuilder(valuesFormed);

        for (int i = 0; i < values.length; i++) {
            T value = values[i];
            StringBuilder addBuilder = new StringBuilder();

            if (value instanceof String) {
                addBuilder.append(stringify(value.toString()));
            } else {
                valuesFormedBuilder.append(value.toString());
            }

            valuesFormedBuilder.append(addBuilder.toString());

            if (i < values.length - 1)
                valuesFormedBuilder.append(",");
        }

        valuesFormedBuilder.append(')');

        return valuesFormedBuilder.toString();
    }

    void loadProducesFromFile(File data) throws FileNotFoundException, SQLException {
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

    void showTables() {
        try {
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void addToDatabase(String secondWord, String values) throws SQLException {
        String tableName = secondWord.substring(0, secondWord.length() - 1);

        if (tableName.equalsIgnoreCase("FARMER"))
            addFarmers(values);
        else if (tableName.equalsIgnoreCase("PRODUCT"))
            addProducts(values);
        else if (tableName.equalsIgnoreCase("MARKET"))
            addMarkets(values);
    }

    void addFarmers(String values) throws SQLException {
        // Cache the keys to query the inserted data to delete in case of failure
        ArrayList<Integer> farmerIds = new ArrayList<>();

        try {
            String[] valueSets = values.split(":");
            for (String valueSet : valueSets) {
                valueSet = valueSet.substring(1, valueSet.length() - 1);
                String[] valuesSeparated = valueSet.split(",");

                String name = valuesSeparated[0];
                String lastName = valuesSeparated[1];
                String zipCode = valuesSeparated[2];
                String city = valuesSeparated[3];
                String[] phoneNumbers = valuesSeparated[4].split(";");
                String[] emails = valuesSeparated[5].split(";");

                farmerIds.add(nextFarmerId + 1);

                insertFarmerRelatedData(name, lastName, zipCode, city, phoneNumbers, emails);
            }
        } catch (Exception e) {
            System.out.println("A critical error has occurred during an insertion, aborting for the sake of atomicity");
            deleteFarmersWithId(farmerIds);
            e.printStackTrace();
        }
    }

    private void deleteFarmersWithId(ArrayList<Integer> farmerIds) throws SQLException {
        // Create the query body to avoid unnecessary string operations in for loop
        String sql = "DELETE FROM Farmer " +
                     "WHERE f_id = ";

        for (int farmerId : farmerIds) {
             statement.executeUpdate(sql + farmerId);
        }
    }

    private void insertFarmerRelatedData(String name, String lastName, String zipCode, String city, String[] phoneNumbers, String[] emails) throws SQLException {
        insertFarmer(name, lastName);
        insertFarmerAddressZipcode("", zipCode);
        insertFarmerAddressCity(city, "");
        insertFarmerPhoneNumbers(phoneNumbers);
        insertFarmerEmails(emails);
    }

    private void addProducts(String values) throws SQLException {
        // Cache the keys to query the inserted data to delete in case of failure
        ArrayList<Integer> productIds = new ArrayList<>();

        try {
            String[] valueSets = values.split(":");

            for (String valueSet : valueSets) {
                valueSet = valueSet.substring(1, valueSet.length() - 1);
                String[] valuesSeparated = valueSet.split(",");

                String productName = valuesSeparated[0];
                String plantDate = valuesSeparated[1];
                String harvestDate = valuesSeparated[2];
                int hardness = Integer.parseInt(valuesSeparated[3]);
                int altitude = Integer.parseInt(valuesSeparated[4]);
                int minTemp = Integer.parseInt(valuesSeparated[5]);

                productIds.add(nextProductId + 1);

                insertProductRelatedData(productName, plantDate, harvestDate, hardness, altitude, minTemp);
            }
        } catch (Exception e) {
            System.out.println("A critical error has occurred during an insertion, aborting for the sake of atomicity");
            deleteProducts(productIds);
            e.printStackTrace();
        }
    }

    private void insertProductRelatedData(String productName, String plantDate, String harvestDate,
                                          int hardness, int altitude, int minTemp) throws SQLException {
        insertProduct(productName, hardness);
        insertPlantDate_HarvestDate(plantDate, harvestDate);
        insertAltLevel_MinTemp(altitude, minTemp);
    }

    private void deleteProducts(ArrayList<Integer> productIds) throws SQLException {
        String sql = "DELETE FROM Product " +
                     "WHERE p_id = ";

        for (int productId : productIds) {
            statement.executeUpdate(sql + productId);
        }
    }


    private void addMarkets(String values) throws SQLException {
        // Cache the keys to query the inserted data to delete in case of failure
        ArrayList<Integer> marketIds = new ArrayList<>();

        try {
            String[] valueSets = values.split(":");

            for (String valueSet : valueSets) {
                valueSet = valueSet.substring(1, valueSet.length() - 1);
                String[] valuesSeparated = valueSet.split(",");
                String marketName = valuesSeparated[0];
                String marketAddress = valuesSeparated[1];
                String zip_code = valuesSeparated[2];
                String city = valuesSeparated[3];
                String[] phones = valuesSeparated[4].split(";");
                int budget = Integer.parseInt(valuesSeparated[5]);

                marketIds.add(nextMarketId + 1);

                insertMarketRelatedData(marketName, marketAddress, zip_code, city, budget, phones);
            }
        } catch (Exception e) {
            System.out.println("A critical error has occurred during an insertion, aborting for the sake of atomicity");
            deleteMarkets(marketIds);
            e.printStackTrace();
        }
    }

    private void insertMarketRelatedData(String marketName, String marketAddress,
                                         String zipcode, String city,
                                         int budget, String[] phoneNumbers) throws SQLException {
        insertMarket(marketName, marketAddress, budget);
        insertMarketAddressZipcode(marketAddress, zipcode);
        insertMarketAddressCity(marketAddress, city);
        insertMarketPhoneNumber(phoneNumbers);
    }

    private void deleteMarkets(ArrayList<Integer> marketIds) throws SQLException {
        String sql = "DELETE FROM Market " +
                     "WHERE m_id = ";

        for (int marketId : marketIds) {
            statement.executeUpdate(sql + marketId);
        }
    }

    void insertMarketPhoneNumber(String[] phoneNumbers) throws SQLException {
        for (String phone : phoneNumbers) {
            long phoneNumber = Long.valueOf(phone);

            String sql = "INSERT INTO MarketPhoneNumber "
                       + "VALUES " + convertToMysqlValuesForm(phoneNumber, nextMarketId);
            statement.executeUpdate(sql);
        }
    }
}


