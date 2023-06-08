package gob.mdmq.mdmq_cmi_masivo.components;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import gob.mdmq.mdmq_cmi_masivo.model.datos;



@Component
public class Producer {

    /* @Value("${topic.name}")
    private String orderTopic; */

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public Producer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public String sendMessage(datos datos, String topic) throws JsonProcessingException {
        String orderAsMessage = objectMapper.writeValueAsString(datos);
        kafkaTemplate.send(topic, orderAsMessage);

        return "message sent";
    }

}
