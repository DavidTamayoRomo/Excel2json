package gob.mdmq.mdmq_cmi_masivo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.JsonObject;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import gob.mdmq.mdmq_cmi_masivo.model.File;
import gob.mdmq.mdmq_cmi_masivo.model.datos;
import gob.mdmq.mdmq_cmi_masivo.service.datosService;

import org.apache.poi.ss.usermodel.*;
import org.bson.Document;
import org.apache.poi.poifs.filesystem.OfficeXmlFileException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;

@RestController
@RequestMapping("/masivo")
public class datosController {

    private final datosService datosOrderService;

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final MongoTemplate mongoTemplate;

    @Autowired
    public datosController(KafkaTemplate<String, String> kafkaTemplate, datosService datosOrderService,
            MongoTemplate mongoTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.datosOrderService = datosOrderService;
        this.mongoTemplate = mongoTemplate;
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

    @PostMapping("/upload1")
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

    @PostMapping(value = "/uploadcsv1")
    public void uploadFileCSV(@RequestParam("file") MultipartFile file, @RequestParam Boolean coma) {
        try {
            InputStream in = file.getInputStream();
            InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
            if (coma) {
                CSVParser csvParser = new CSVParserBuilder()
                        .withSeparator(',').withIgnoreQuotations(false).build();
                CSVReader csvReader = new CSVReaderBuilder(reader)
                        // .withSkipLines(1) // Omitir la primera línea si es un encabezado
                        .withCSVParser(csvParser)
                        .build();

                // CSVReader csvReader = new CSVReader(reader);

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

            } else {
                CSVParser csvParser = new CSVParserBuilder()
                        .withSeparator(';').withIgnoreQuotations(false).build(); // Delimitador de punto y coma
                CSVReader csvReader = new CSVReaderBuilder(reader)
                        // .withSkipLines(1) // Omitir la primera línea si es un encabezado
                        .withCSVParser(csvParser)
                        .build();

                // CSVReader csvReader = new CSVReader(reader);

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

            }
            /* */
            // Delimitador de coma

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Async
    private void processBatch(int currentBatch, List<JsonObject> datosFinales) {
        // System.out.println("datosFinales " + datosFinales);
        try {

            for (JsonObject datosFinal : datosFinales) {
                try {
                    sendMessage(datosFinal.toString(), "temaBroker-2");
                } catch (Exception e) {
                    System.out.println("Error al enviar el mensaje");
                }

            }
            // Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String sendMessage(String datos, String topic) throws JsonProcessingException {
        kafkaTemplate.send(topic, datos);
        return "message sent";
    }

    @PostMapping(value = "/uploadcsvBase641")
    public void uploadFileCSVBase64(@RequestBody File file, @RequestParam Boolean coma) {
        try {

            String base64File = file.getFile();
            byte[] decodedBytes = Base64.getDecoder().decode(base64File);
            InputStream in = new ByteArrayInputStream(decodedBytes);
            InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8);

            if (coma) {
                CSVParser csvParser = new CSVParserBuilder()
                        .withSeparator(',').withIgnoreQuotations(false).build();
                CSVReader csvReader = new CSVReaderBuilder(reader)
                        // .withSkipLines(1) // Omitir la primera línea si es un encabezado
                        .withCSVParser(csvParser)
                        .build();

                // CSVReader csvReader = new CSVReader(reader);

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

            } else {
                CSVParser csvParser = new CSVParserBuilder()
                        .withSeparator(';').withIgnoreQuotations(false).build(); // Delimitador de punto y coma
                CSVReader csvReader = new CSVReaderBuilder(reader)
                        // .withSkipLines(1) // Omitir la primera línea si es un encabezado
                        .withCSVParser(csvParser)
                        .build();

                // CSVReader csvReader = new CSVReader(reader);

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

            }
            /* */
            // Delimitador de coma

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @PostMapping(value = "/uploadcsvBase64many1")
    public void uploadFileCSVBase64many(@RequestBody File file, @RequestParam Boolean coma) {
        try {

            String base64File = file.getFile();
            byte[] decodedBytes = Base64.getDecoder().decode(base64File);
            InputStream in = new ByteArrayInputStream(decodedBytes);
            InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8);

            CSVParser csvParser = new CSVParserBuilder().withSeparator(',').withIgnoreQuotations(false).build();

            try (CSVReader csvReader = new CSVReaderBuilder(reader).withCSVParser(csvParser).build()) {

                List<String> headers = null;
                List<JsonObject> datosFinales = new ArrayList<>();

                int batchSize = 1000;
                int currentBatch = 0;

                String[] nextLine;
                Integer contador = 0;
                while ((nextLine = csvReader.readNext()) != null) {

                    if (currentBatch == batchSize) {
                        processBatch(currentBatch, datosFinales);
                        // collection.insertMany(batch);
                        // batch.clear();
                        this.mongoTemplate.insertAll(datosFinales);
                        currentBatch = 0;
                        datosFinales = new ArrayList<>();
                        System.out.println("Datos Procesados: " + contador);
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

            }

            /* */
            // Delimitador de coma

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //Estos son los que se van a usar
    @PostMapping("/uploadfileCSV")
    public void uploadfileCSV(@RequestParam("file") MultipartFile file, @RequestParam(required = true) String sistema) {
        try (
                InputStream in = file.getInputStream();
                InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
                CSVReader csvReader = new CSVReaderBuilder(reader)
                        .withCSVParser(new CSVParserBuilder().withSeparator(';').withIgnoreQuotations(false).build())
                        .build()) {

            List<String> headers = null;
            List<Document> datosFinales = new ArrayList<>();

            int batchSize = 50000;
            int currentBatch = 0;

            String[] nextLine;
            Integer contador = 0;
            while ((nextLine = csvReader.readNext()) != null) {
                if (currentBatch == batchSize) {
                    // almacenenar en la base de datos
                    this.mongoTemplate.insert(datosFinales, sistema);
                    System.out.println("Datos Procesados: " + contador);
                    currentBatch = 0;
                    datosFinales.clear();
                }

                if (headers == null) {
                    headers = Arrays.asList(nextLine);
                } else {
                    Document document = new Document();
                    for (int i = 0; i < headers.size(); i++) {
                        String header = headers.get(i);
                        String value = nextLine[i];
                        document.put(header, value);
                    }
                    Document outerDocument = new Document();
                    outerDocument.put("datos", document);
                    datosFinales.add(outerDocument);
                }

                currentBatch++;
                contador++;
            }

            if (currentBatch > 0) {
                // processBatch(currentBatch, datosFinales);
                this.mongoTemplate.insert(datosFinales, sistema);
                System.out.println("Datos Procesados: " + contador);
                currentBatch = 0;
                datosFinales.clear();
            }

        } catch (OfficeXmlFileException e) {
            e.printStackTrace();
            System.out.println("File type mismatch");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @PostMapping("/uploadfileCSVBase64")
    public void uploadfileCSVBase64(@RequestBody File file, @RequestParam(required = true) String sistema) {
        String base64File = file.getFile();
        byte[] decodedBytes = Base64.getDecoder().decode(base64File);
        try (
                
                InputStream in = new ByteArrayInputStream(decodedBytes);
                InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
                CSVReader csvReader = new CSVReaderBuilder(reader)
                        .withCSVParser(new CSVParserBuilder().withSeparator(';').withIgnoreQuotations(false).build())
                        .build()) {

            List<String> headers = null;
            List<Document> datosFinales = new ArrayList<>();

            int batchSize = 50000;
            int currentBatch = 0;

            String[] nextLine;
            Integer contador = 0;
            while ((nextLine = csvReader.readNext()) != null) {
                if (currentBatch == batchSize) {
                    // almacenenar en la base de datos
                    this.mongoTemplate.insert(datosFinales, sistema);
                    System.out.println("Datos Procesados: " + contador);
                    currentBatch = 0;
                    datosFinales.clear();
                }

                if (headers == null) {
                    headers = Arrays.asList(nextLine);
                } else {
                    Document document = new Document();
                    for (int i = 0; i < headers.size(); i++) {
                        String header = headers.get(i);
                        String value = nextLine[i];
                        document.put(header, value);
                    }
                    Document outerDocument = new Document();
                    outerDocument.put("datos", document);
                    datosFinales.add(outerDocument);
                }

                currentBatch++;
                contador++;
            }

            if (currentBatch > 0) {
                // processBatch(currentBatch, datosFinales);
                this.mongoTemplate.insert(datosFinales, sistema);
                System.out.println("Datos Procesados: " + contador);
                currentBatch = 0;
                datosFinales.clear();
            }

        } catch (OfficeXmlFileException e) {
            e.printStackTrace();
            System.out.println("File type mismatch");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
