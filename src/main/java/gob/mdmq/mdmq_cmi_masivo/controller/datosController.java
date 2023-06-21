package gob.mdmq.mdmq_cmi_masivo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import gob.mdmq.mdmq_cmi_masivo.model.datos;
import gob.mdmq.mdmq_cmi_masivo.service.datosService;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.OfficeXmlFileException;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/masivo")
public class datosController {

    private final datosService datosOrderService;

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public datosController(KafkaTemplate<String, String> kafkaTemplate, datosService datosOrderService) {
        this.kafkaTemplate = kafkaTemplate;
        this.datosOrderService = datosOrderService;
    }

    @PostMapping
    public String createDatosOrder(@RequestBody datos datosBody, @RequestParam String topic)
            throws JsonProcessingException {

        try {

            datosOrderService.createDatosOrder(datosBody, topic);

        } catch (Exception e) {
            return "Error al enviar el mensaje";
        }

        return "Mensaje enviado";

    }

    @PostMapping("/upload")
    public void uploadFile(@RequestParam("file") MultipartFile file) {
        try {
            InputStream in = file.getInputStream();

            List<String> datos1 = new ArrayList<String>();
            // Usamos WorkbookFactory en lugar de HSSFWorkbook
            Workbook workbook = WorkbookFactory.create(in);
            Sheet sheet = workbook.getSheetAt(0);
            Iterator<Row> rows = sheet.iterator();

            Row headerRow = sheet.getRow(0);
            Iterator<Cell> headerCells = headerRow.cellIterator();

            while (headerCells.hasNext()) {
                Cell headerCell = headerCells.next();
                String headerValue = headerCell.getStringCellValue();
                datos1.add(headerValue);
            }

            int batchSize = 1000;
            int currentBatch = 0;
            List<JsonObject> datosFinales = new ArrayList<JsonObject>();
            while (rows.hasNext()) {
                if (currentBatch == batchSize) {
                    processBatch(currentBatch, datosFinales);
                    currentBatch = 0;
                    datosFinales = new ArrayList<JsonObject>();
                }
                Row row = rows.next();
                Iterator<Cell> cells = row.cellIterator();

                int i = 0;
                // Crear objeto Json
                JsonObject jsonObject = new JsonObject();

                while (cells.hasNext()) {
                    Cell cell = cells.next();
                    jsonObject.addProperty(datos1.get(i), cell.toString());
                    System.out.println("jsonObject: " + jsonObject);
                    i++;
                }

                // objeto2.put("datos", objeto);
                datosFinales.add(jsonObject);

                currentBatch++;
            }

            if (currentBatch > 0) {
                processBatch(currentBatch, datosFinales);
            }

        } catch (OfficeXmlFileException e) {
            e.printStackTrace();
            System.out.println("File type mismatch");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @PostMapping("/uploadcsv")
    public void uploadFileCSV(@RequestParam("file") MultipartFile file) {
        try {
            InputStream in = file.getInputStream();
            InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8);

            /* CSVParser csvParser = new CSVParserBuilder()
                .withSeparator(',').withIgnoreQuotations(false).build(); */ // Delimitador de coma
            CSVParser csvParser = new CSVParserBuilder()
                .withSeparator(';').withIgnoreQuotations(false).build(); // Delimitador de punto y coma

            CSVReader csvReader = new CSVReaderBuilder(reader)
                //.withSkipLines(1) // Omitir la primera línea si es un encabezado
                .withCSVParser(csvParser)
                .build();

            //CSVReader csvReader = new CSVReader(reader);

            List<String> headers = null;
            List<JsonObject> datosFinales = new ArrayList<>();

            int batchSize = 1000;
            int currentBatch = 0;

            String[] nextLine;
            while ((nextLine = csvReader.readNext()) != null) {
                if (currentBatch == batchSize) {
                    processBatch(currentBatch, datosFinales);
                    currentBatch = 0;
                    datosFinales = new ArrayList<>();
                }

                if (headers == null) {
                    headers = Arrays.asList(nextLine);
                } else {
                    JsonObject jsonObject = new JsonObject();
                    for (int i = 0; i < headers.size(); i++) {
                        String header = headers.get(i);
                        String value = nextLine[i];
                        jsonObject.addProperty(header, value);
                        System.out.println("jsonObject: " + jsonObject);
                    }
                    datosFinales.add(jsonObject);
                }

                currentBatch++;
            }

            if (currentBatch > 0) {
                processBatch(currentBatch, datosFinales);
            }

            csvReader.close();
            reader.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Async
    private void processBatch(int currentBatch, List<JsonObject> datosFinales) {
        System.out.println("datosFinales " + datosFinales);
        try {

            for (JsonObject datosFinal : datosFinales) {
                try {
                    sendMessage(datosFinal.toString(), "temaBroker-2");
                } catch (Exception e) {
                    System.out.println("Error al enviar el mensaje");
                }

            }
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String sendMessage(String datos, String topic) throws JsonProcessingException {
        kafkaTemplate.send(topic, datos);
        return "message sent";
    }

}
