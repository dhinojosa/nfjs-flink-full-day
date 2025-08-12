package com.evolutionnext;

import org.postgresql.ds.PGConnectionPoolDataSource;

import java.sql.*;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class CreateStocks {
    public static void main(String[] args) throws InterruptedException {
        PGConnectionPoolDataSource source = new PGConnectionPoolDataSource();
        source.setURL("jdbc:postgresql://localhost:5432/");
        source.setUser("docker");
        source.setPassword("docker");
        source.setDatabaseName("docker");

        List<String> stockSymbols = List.of("ADBE", "ADI", "ADP", "ADSK", "AEP", "ALGN", "AMAT",
            "AMGN", "ANSS", "ASML", "ATVI", "AVGO", "AZN", "BIIB", "BKNG", "BKR", "CDNS", "CEG",
            "CHTR", "CMCSA", "COST", "CPRT", "CRWD", "CSGP", "CSX", "CTAS", "CTSH", "DDOG", "DLTR",
            "DXCM", "EA", "EBAY", "ENPH", "EXC", "FAST", "FISV", "FTNT", "GFS", "GILD", "GOOG", "GOOGL",
            "HON", "IDXX", "ILMN", "INTU", "ISRG", "KDP", "KHC", "KLAC", "LCID", "LRCX", "LULU", "MAR", "MCHP",
            "MDLZ", "MNST", "MRNA", "MRVL", "NFLX", "NXPI", "ODFL", "ORLY", "PANW", "PAYX", "PCAR",
            "PDD", "PYPL", "QCOM", "REGN", "ROST", "SBUX", "SGEN", "SIRI", "SNPS", "TEAM", "TMUS",
            "TXN", "VRSK", "VRTX", "WBA", "WBD", "WDAY", "XEL", "ZM", "ZS");

        AtomicBoolean done = new AtomicBoolean(false);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> done.set(true)));

        Random random = new Random();
        while (!done.get()) {
            try (Connection connection = source.getConnection()) {
                connection.setAutoCommit(false);
                try (PreparedStatement preparedStatementState =
                         connection.prepareStatement
                             ("INSERT INTO stock_trade (stock_symbol, trade_timestamp, trade_type, amount) values (?, ?, ?, ?);",
                                 Statement.RETURN_GENERATED_KEYS)) {
                    preparedStatementState.setString(1, stockSymbols.get(random.nextInt(stockSymbols.size() - 1)));
                    preparedStatementState.setTimestamp(2, new Timestamp(Instant.now().toEpochMilli()));
                    preparedStatementState.setString(3, random.nextBoolean() ? "buy" : "sell");
                    preparedStatementState.setFloat(4, random.nextFloat() *  1000);
                    preparedStatementState.execute();

                    ResultSet generatedKeys = preparedStatementState.getGeneratedKeys();
                    generatedKeys.next();
                    System.out.println(generatedKeys.getLong(1));
                    Thread.sleep(random.nextInt(10000 - 5000) + 5000);
                    connection.commit();
                } catch (SQLException sqlException) {
                    connection.rollback();
                    sqlException.printStackTrace();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
