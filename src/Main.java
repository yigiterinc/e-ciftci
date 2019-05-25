import java.io.File;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        File farmers = new File("src/Resources/farmers.csv");
        File transactions = new File("src/Resources/buys.csv");
        File markets = new File("src/Resources/markets.csv");
        File registers = new File("src/Resources/registers.csv");
        File products = new File("src/Resources/products.csv");
        File produces = new File("src/Resources/produces.csv");

        try {
            Session session = new Session("134.209.246.115", 3306, "eciftci",
                    "root", "Ybcs202_2019");

            // session.showTables();
            // session.insertFarmersFromFile(farmers);
            // session.insertMarketsFromFile(markets);
            // session.insertProductsFromFile(products);
            //session.insertProducesFromFile(produces);
            //session.insertRegistersFromFile(registers);
            //session.insertTransactionsFromFile(transactions);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
