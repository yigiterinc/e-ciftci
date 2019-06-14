import java.sql.SQLException;
import java.util.Scanner;

// Yiğit Kemal Erinç, Boran Pekel

public class Main {
    private static final String IP = "134.209.246.115";
    private static final int MysqlPort = 3306;
    private static final String databaseName = "eciftci";

    public static void main(String[] args) throws SQLException {
        System.out.println("Loading the data from files...");

        Session session = new Session(IP, MysqlPort, databaseName,
                "root", "Ybcs202_2019");

        Scanner scanner = new Scanner(System.in);

        System.out.println("Welcome, type in 'LOAD DATA' to load the data from Resources\n" +
                           "and 'SHOW TABLES' to see the tables\n" +
                           "'ADD X(values)' for single entry or 'ADD Xs(values) to insert multiple entries\n" +
                           "it is assumed that multi values like email will be separated with ; \n" +
                           "You can use 'QUERY X' where X is the number of the query (1-5) " +
                           "to run, type 'EXIT' to exit.");

        while (true) {
            String input = scanner.nextLine();
            String firstWord = input.split(" ")[0];
            String secondWord = input.replaceAll(" ", "").split("\\(")[0]
                                     .substring(firstWord.length());
            int startIndexOfValues = firstWord.length() + secondWord.length();
            String values = input.replaceAll(" ", "")
                                 .substring(startIndexOfValues);

            if (firstWord.equalsIgnoreCase("load")) {
                session.loadFromFiles();
            } else if (firstWord.equalsIgnoreCase("query")) {
                session.runQuery(secondWord);
            } else if(input.equalsIgnoreCase("show tables")) {
                session.showTables();
            } else if (firstWord.equalsIgnoreCase("exit")) {
                System.out.println("Goodbye");
                System.exit(0);
            } else if (firstWord.equalsIgnoreCase("add") || firstWord.equalsIgnoreCase("register")) {
                session.addToDatabase(secondWord, values);
            }
        }
    }
}
