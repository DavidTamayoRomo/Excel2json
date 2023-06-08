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

import gob.mdmq.mdmq_cmi_masivo.model.datos;
import gob.mdmq.mdmq_cmi_masivo.service.datosService;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.OfficeXmlFileException;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import java.io.InputStream;
import java.util.ArrayList;
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
            List<Map<String, Object>> datosFinales = new ArrayList<Map<String, Object>>();
            while (rows.hasNext()) {
                if (currentBatch == batchSize) {
                    processBatch(currentBatch, datosFinales);
                    currentBatch = 0;
                }
                Row row = rows.next();
                Iterator<Cell> cells = row.cellIterator();

                int i = 0;
                Map<String, Object> objeto = new HashMap<>();
                Map<String, Object> objeto2 = new HashMap<>();
                
                
                while (cells.hasNext()) {
                    Cell cell = cells.next();
                    objeto.put(datos1.get(i), cell);

                    i++;
                }

                //objeto2.put("datos", objeto);
                datosFinales.add(objeto);

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

    @Async
    private void processBatch(int currentBatch, List<Map<String, Object>> datosFinales) {
        System.out.println("datosFinales " + datosFinales);
        try {
            /*
             * var v = datosFinales.iterator().next();
             * 
             * Map<String, Object> obj = new HashMap<>();
             * obj = (Map<String, Object>) datosFinales.get(0);
             * var y= obj.get("datos").toString().substring(0);
             * 
             * Gson gson = new GsonBuilder().setPrettyPrinting().create();
             * 
             * String rpt1 = gson.toJson(y.substring(0));
             * 
             * 
             * ObjectMapper Obj1 = new ObjectMapper();
             * String rpt = Obj1.writeValueAsString(rpt1);
             * 
             * 
             * sendMessage(rpt, "temaBroker-2");
             */

            for (Object datosFinal : datosFinales) {
                try {

                    /* var t = obj.get(datosFinales.get(0)); */



                    //Map<String, Object> obj = new HashMap<>();
                    //obj = (Map<String, Object>) datosFinal;
                    Gson gson = new Gson();
                    String objetoAsJson = gson.toJson(datosFinal);
                    System.out.println(objetoAsJson);
                    // ObjectMapper Obj = new ObjectMapper();
                    // String jsonStr = Obj.writeValueAsString(datosFinal);
                    // System.out.println(jsonStr);
                    sendMessage(objetoAsJson, "temaBroker-2");
                } catch (Exception e) {
                    System.out.println("Error al enviar el mensaje");
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String sendMessage(String datos, String topic) throws JsonProcessingException {
        kafkaTemplate.send(topic, datos);
        return "message sent";
    }

}
