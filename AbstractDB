import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public abstract class AbstractDB {

    private static final Log log = LogFactory.getLog(AbstractDB.class);

    protected static final String DEFAULT_TABLE_NAME = "abstract_db";

    protected DBConnection dbConnection;
    protected String tableName;

    public AbstractDB(DBConnection dbConnection) {
        this(dbConnection, DEFAULT_TABLE_NAME);
    }

    public AbstractDB(DBConnection dbConnection, String tableName) {
        this.dbConnection = dbConnection;
        this.tableName = tableName;
    }

    protected abstract String getCreateTableSql();

    public void createTable() {
        try {
            Statement statement = dbConnection.getConnection().createStatement();
            statement.execute(getCreateTableSql());
            statement.close();
        }
        catch (SQLException e) {
            log.error(e);
        }
    }

    protected <T> List<T> queryToList(Class<T> clazz, String sql, Object... parameters) {
        QueryRunner queryRunner=new QueryRunner();
        List<T> list=new ArrayList<>();
        try {
            list = queryRunner.query(dbConnection.getConnection(), sql, new BeanListHandler<>(clazz), parameters);
        }
        catch (SQLException e) {
            log.error(e);
        }
        return list;
    }

    protected <T> long insert(Class<T> type, final T t, String... ignore) {
        Map<String, Method> map = Utils.getFieldsWithGetter(type);

        HashSet<String> ignoreSet = new HashSet<>(Arrays.asList(ignore));

        List<String> fields = new ArrayList<>();
        List<String> queries = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        map.forEach((x,y)->{
            if (ignoreSet.contains(x)) return;
            try {
                values.add(y.invoke(t));
                fields.add(x);
                queries.add("?");
            } catch (Exception e) {
                log.error(e);
            }
        });

        if (!fields.isEmpty()) {
            String sql = "insert into `"+tableName+"` ("
                    + String.join(", ", fields)
                    + ") values ("
                    + String.join(", ", queries)
                    + ")";
            QueryRunner queryRunner = new QueryRunner();
            try {
                return queryRunner.insert(dbConnection.getConnection(), sql, new ScalarHandler<>(), values.toArray());
            }
            catch (SQLException e) {
                log.error(e);
            }
        }
        return 0;
    }

    protected <T> void update(Class<T> type, T t, String primaryKey, String... updateColumns) {
        Map<String, Method> map = Utils.getFieldsWithGetter(type);

        HashSet<String> columns = new HashSet<>(Arrays.asList(updateColumns));

        List<String> fields = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        Object[] primaryValues = new Object[1];

        map.forEach((x,y)->{
            if (!x.equals(primaryKey) && !columns.contains(x)) return;
            Object value;
            try {
                value = y.invoke(t);
            }
            catch (Exception e) {
                log.error(e);
                return;
            }
            if (x.equals(primaryKey)) {
                primaryValues[0] = value;
            }
            else {
                fields.add(x+"=?");
                values.add(value);
            }
        });

        values.add(primaryValues[0]);

        if (!fields.isEmpty()) {
            String sql = "update `" + tableName + "` set "
                    + String.join(", ", fields)
                    + " where " + primaryKey + "=?";
            QueryRunner queryRunner=new QueryRunner();
            try {
                queryRunner.update(dbConnection.getConnection(), sql, values.toArray());
            } catch (SQLException e) {
                log.error(e);
            }
        }
    }



}
