package com.bfi.riskmanagement.controllers;


import com.bfi.riskmanagement.services.ImportData;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.transaction.Transactional;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

@RestController
@AllArgsConstructor
@CrossOrigin(origins = "*")
public class ImportControllers {


    @Autowired
    private ImportData importData;


    //http://localhost:8089/PFE/upload
    @PostMapping(value ="/upload", consumes = {"multipart/form-data"})
    @Transactional
    public ResponseEntity<String> handleFileUpload(@RequestParam("file") MultipartFile file, @RequestParam("separator") String separator, @RequestParam("tableName") String tableName) {
        // Check if the file is empty
        if (file.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please select a file to upload.");
        }

        try {
            // Parse the CSV file and save its data to the database
            importData.importDataFromCsv(file, separator, tableName);
            return ResponseEntity.status(HttpStatus.OK).body("Data imported successfully.");
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error importing data: " + e.getMessage());
        }
    }

    //http://localhost:8089/PFE/{{tableName}}
    @GetMapping("/{tableName}")
    public ResponseEntity<List<List<String>>> getImportedData(@PathVariable String tableName) {
        List<List<String>> importedData = importData.getImportedData(tableName);
        return ResponseEntity.ok(importedData);
    }

    //http://localhost:8089/PFE/{{tableName}}/columns
    @GetMapping("/{tableName}/columns")
        public String[] GetColumns(@PathVariable String tableName) throws SQLException {
            return importData.getColumns(tableName);
        }


    //http://localhost:8089/PFE/data/{{tableName}}/{{primaryKeyValue}}
    @GetMapping("/data/{tableName}/{primaryKeyValue}")
    public Object retrieveDataByPrimaryKey(@PathVariable String tableName, @PathVariable String primaryKeyValue) throws SQLException {
        return importData.retrieveDataByPrimaryKey(tableName, primaryKeyValue);
    }

    //http://localhost:8089/PFE/{{tableName}}/add
    @PostMapping("/{tableName}/add")
    public ResponseEntity<String> addData(@PathVariable String tableName, @RequestBody String[] newData) {
        importData.addData(tableName, newData);
        return ResponseEntity.status(HttpStatus.CREATED).body("Data added successfully.");
    }

    //http://localhost:8089/PFE/data/{{tableName}}/{{primaryKeyValue}}
    @PutMapping("/data/{tableName}/{primaryKeyValue}")
    public void updateData(@PathVariable String tableName, @PathVariable String primaryKeyValue, @RequestBody String[] newData, HttpServletResponse response) throws IOException, SQLException {
        try {
            importData.updateData(tableName, primaryKeyValue, newData);
            response.setStatus(HttpServletResponse.SC_OK);
        } catch (SQLException e) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Error updating data: " + e.getMessage());
        }
    }

    //http://localhost:8089/PFE/data/{{tableName}}/{{primaryKeyValue}}
    @DeleteMapping("/data/{tableName}/{primaryKeyValue}")
    public void deleteData(@PathVariable String tableName, @PathVariable String primaryKeyValue, HttpServletResponse response) throws IOException, SQLException {
        try {
            importData.deleteData(tableName, primaryKeyValue);
            response.setStatus(HttpServletResponse.SC_OK);
        } catch (SQLException e) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Error deleting data: " + e.getMessage());
        }
    }


}
