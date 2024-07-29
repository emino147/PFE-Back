package com.bfi.riskmanagement.services;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import jakarta.transaction.Transactional;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
@AllArgsConstructor
public class ImportData {

    private static final Logger logger = LoggerFactory.getLogger(ImportData.class);

    @PersistenceContext
    private EntityManager entityManager;

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public ImportData(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }


    public void importDataFromCsv(MultipartFile file, String separator, String tableName) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            // Read the header row
            String header = reader.readLine();

            // Create a table dynamically based on the header
            createTable(header, separator, tableName, file);

            // Read and process data rows
            String line;
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(separator);
                System.out.println(Arrays.toString(data));
                insertData(data, tableName);
            }
        }
    }

    private void createTable(String header, String separator, String tableName, MultipartFile file) {
        // Drop the table if it already exists
        StringBuilder dropTableQueryBuilder = new StringBuilder("DROP TABLE IF EXISTS ");
        dropTableQueryBuilder.append(tableName);

        Query dropTableQuery = entityManager.createNativeQuery(dropTableQueryBuilder.toString());
        dropTableQuery.executeUpdate();

        StringBuilder queryBuilder = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        queryBuilder.append(tableName).append(" (");

        String[] columns = header.split(separator);
        System.out.println(Arrays.toString(columns));

        String[] dataTypes = determineDataTypes3(file, separator);
        System.out.println(Arrays.toString(dataTypes));

        for (int i = 0; i < columns.length; i++) {
            String column = columns[i].trim();
            String dataType = dataTypes[i].trim();
            queryBuilder.append("`").append(column).append("` ").append(dataType);

            if (i == 0) {
                queryBuilder.append(" PRIMARY KEY");
            }

            queryBuilder.append(",");

        }
        queryBuilder.setLength(queryBuilder.length() - 1);
        queryBuilder.append(")");

        Query query = entityManager.createNativeQuery(queryBuilder.toString());
        query.executeUpdate();
    }

    private void insertData(String[] data, String tableName) {
        StringBuilder queryBuilder = new StringBuilder("INSERT INTO ");
        queryBuilder.append(tableName).append(" VALUES (");
        for (String value : data) {
            queryBuilder.append("'").append(value.trim()).append("',");
        }
        queryBuilder.setLength(queryBuilder.length() - 1);
        queryBuilder.append(")");

        Query query = entityManager.createNativeQuery(queryBuilder.toString());
        query.executeUpdate();
    }

    private String[] determineDataTypes3(MultipartFile file, String separator) {
        String[] data = new String[100];
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            String line;
            reader.readLine();
            line = reader.readLine();

            data = line.split(separator);

            for (int i = 0; i < data.length; i++){
                    if (TypeChecker.isBigInt(data[i])){
                        data[i] = "BIGINT";
                    } else if (TypeChecker.isDouble(data[i])){
                        data[i] = "DOUBLE";
                    } else if (TypeChecker.isBoolean(data[i])) {
                        data[i] = "BOOLEAN";
                    } else if (TypeChecker.isDate((data[i]))){
                        data[i] = "DATE";
                    } else {
                        data[i] = "TEXT";
                    }
            }

        } catch (IOException e) {
            logger.error("Error occurred while reading the file: {}", e.getMessage());
        }

        return data;
    }

    public List<List<String>> getImportedData(String tableName) {
        List<List<String>> importedData = new ArrayList<>();
        String queryString = "SELECT * FROM " + tableName;
        Query query = entityManager.createNativeQuery(queryString);
        List<Object[]> results = query.getResultList();
        for (Object[] row : results) {
            List<String> rowData = new ArrayList<>();
            for (Object value : row) {
                rowData.add(value.toString());
            }
            importedData.add(rowData);
        }
        return importedData;
    }

    @Transactional
    public Object retrieveDataByPrimaryKey(String tableName, String primaryKeyValue) throws SQLException {
        String primaryKeyColumnName = getPrimaryKeyColumnName(tableName);
        if (primaryKeyColumnName == null) {
            throw new SQLException("Primary key column not found for table: " + tableName);
        }

        StringBuilder queryString = new StringBuilder("SELECT * FROM ");
        queryString.append(tableName).append(" WHERE ").append(primaryKeyColumnName).append(" = :primaryKeyValue");

        Query query = entityManager.createNativeQuery(queryString.toString());
        query.setParameter("primaryKeyValue", primaryKeyValue);
        return query.getSingleResult();
    }

    @Transactional
    public void addData(String tableName, String[] newData) {
        StringBuilder queryBuilder = new StringBuilder("INSERT INTO ");
        queryBuilder.append(tableName).append(" VALUES (");
        for (String value : newData) {
            queryBuilder.append("'").append(value).append("',");
        }
        queryBuilder.setLength(queryBuilder.length() - 1);
        queryBuilder.append(")");

        Query query = entityManager.createNativeQuery(queryBuilder.toString());
        query.executeUpdate();
    }

    @Transactional
    public void updateData(String tableName, String primaryKeyValue, String[] newData) throws SQLException {
        String primaryKeyColumnName = getPrimaryKeyColumnName(tableName);
        System.out.println(primaryKeyColumnName);
        System.out.println(Arrays.toString(newData));
        if (primaryKeyColumnName == null) {
            throw new SQLException("Primary key column not found for table: " + tableName);
        }

        StringBuilder queryBuilder = new StringBuilder("UPDATE ");
        queryBuilder.append(tableName).append(" SET ");

        String[] columns = getColumns(tableName);
        System.out.println(Arrays.toString(columns));
        for (int i = 0; i < newData.length; i++) {
            queryBuilder.append(columns[i]).append(" = '").append(newData[i]).append("'");
            if (i < newData.length - 1) {
                queryBuilder.append(", ");
            }
        }

        queryBuilder.append(" WHERE ").append(primaryKeyColumnName).append(" = :primaryKeyValue");

        Query query = entityManager.createNativeQuery(queryBuilder.toString());
        query.setParameter("primaryKeyValue", primaryKeyValue);
        query.executeUpdate();
    }

    public String[] getColumns(String tableName) throws SQLException {
        List<String> columns = new ArrayList<>();
        DatabaseMetaData metaData = jdbcTemplate.getDataSource().getConnection().getMetaData();
        try (ResultSet rs = metaData.getColumns(null, null, tableName, null)) {
            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
        }
        return columns.toArray(new String[0]);
    }

    @Transactional
    public void deleteData(String tableName, String primaryKeyValue) throws SQLException {
        String primaryKeyColumnName = getPrimaryKeyColumnName(tableName);
        if (primaryKeyColumnName == null) {
            throw new SQLException("Primary key column not found for table: " + tableName);
        }

        StringBuilder queryString = new StringBuilder("DELETE FROM ");
        queryString.append(tableName).append(" WHERE ").append(primaryKeyColumnName).append(" = :primaryKeyValue");

        Query query = entityManager.createNativeQuery(queryString.toString());
        query.setParameter("primaryKeyValue", primaryKeyValue);
        query.executeUpdate();
    }

    private String getPrimaryKeyColumnName(String tableName) throws SQLException {
        DatabaseMetaData metaData = jdbcTemplate.getDataSource().getConnection().getMetaData();
        try (ResultSet rs = metaData.getPrimaryKeys(null, null, tableName)) {
            if (rs.next()) {
                return rs.getString("COLUMN_NAME");
            }
        }
        return null;
    }


}
