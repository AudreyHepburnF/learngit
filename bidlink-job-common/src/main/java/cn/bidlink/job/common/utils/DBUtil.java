package cn.bidlink.job.common.utils;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static cn.bidlink.job.common.utils.Closer.closeQuietly;

/**
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/8/7
 */
public class DBUtil {
    public static long count(DataSource dataSource, String sql, List<Object> params) {
        List<Map<String, Object>> results = query(dataSource, sql, params);
        return (long) results.get(0).values().iterator().next();
    }

    public static List<Map<String, Object>> query(DataSource dataSource, String sql, List<Object> params) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            preparedStatement = connection.prepareStatement(sql);
            if (params != null && params.size() > 0) {
                int size = params.size();
                for (int i = 0; i < size; i++) {
                    preparedStatement.setObject(i + 1, params.get(i));
                }
            }
            resultSet = preparedStatement.executeQuery();
            List<Map<String, Object>> results = new ArrayList<>();
            while (resultSet.next()) {
                Map<String, Object> map = new LinkedHashMap<>();
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                for (int i = 0; i < columnCount; i++) {
                    String columnLabel = metaData.getColumnLabel(i + 1);
                    map.put(columnLabel, resultSet.getObject(columnLabel));
                }
                results.add(map);
            }

            return results;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeQuietly(resultSet, preparedStatement, connection);
        }
    }

    public static <T> T query(DataSource dataSource, String sql, List<Object> params, ResultSetCallback<T> callback) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            preparedStatement = connection.prepareStatement(sql);
            if (params != null && params.size() > 0) {
                int size = params.size();
                for (int i = 0; i < size; i++) {
                    preparedStatement.setObject(i + 1, params.get(i));
                }
            }
            resultSet = preparedStatement.executeQuery();
            return callback.execute(resultSet);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeQuietly(resultSet, preparedStatement, connection);
        }
    }

    public static void batchExecute(DataSource dataSource, String sql, List<Map<String, Object>> results) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            preparedStatement = connection.prepareStatement(sql);
            for (Map<String, Object> result : results) {
                int i = 0;
                for (Object value : result.values()) {
                    preparedStatement.setObject(i + 1, value);
                    i++;
                }
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeQuietly(resultSet, preparedStatement, connection);
        }
    }

    public static int execute(DataSource dataSource, String sql, List<Object> params) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            preparedStatement = connection.prepareStatement(sql);
            if (params != null && params.size() > 0) {
                int size = params.size();
                for (int i = 0; i < size; i++) {
                    preparedStatement.setObject(i + 1, params.get(i));
                }
            }
            return preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeQuietly(resultSet, preparedStatement, connection);
        }
    }


    public interface ResultSetCallback<T> {
        T execute(ResultSet resultSet) throws SQLException;
    }
}
