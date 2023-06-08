package gob.mdmq.mdmq_cmi_masivo.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;

import gob.mdmq.mdmq_cmi_masivo.components.Producer;
import gob.mdmq.mdmq_cmi_masivo.model.datos;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@Service
public class datosService {
    private Producer producer;
    
    @Autowired
    public datosService(Producer producer) {
        this.producer = producer;
    }

    public String createDatosOrder(datos datosOrder, String topic) throws JsonProcessingException {
        return producer.sendMessage(datosOrder, topic);
    }
}
