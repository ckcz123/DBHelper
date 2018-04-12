import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created by zhang.chen on 4/9/18.
 */
public class DBConnection {

    private static final Log log = LogFactory.getLog(DBConnection.class);

    private String jdbcUrl;
    private String username;
    private String password;

    private Connection connection;
    private RetryableTaskRunner retryableTaskRunner;

    public DBConnection(String jdbcUrl, String username, String password) {
        this.jdbcUrl=jdbcUrl;
        this.username=username;
        this.password=password;

        retryableTaskRunner = new RetryableTaskRunner();

        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            log.error(e);
        }
    }

    public Connection getConnection() {
        try {
            retryableTaskRunner.run(()->{
                if (connection == null || !connection.isValid(5)) {
                    log.debug("Connecting to " + jdbcUrl);
                    connection = DriverManager.getConnection(jdbcUrl, username, password);
                }
            });
        }
        catch (Exception e) {
            connection = null;
        }
        return connection;
    }


}
