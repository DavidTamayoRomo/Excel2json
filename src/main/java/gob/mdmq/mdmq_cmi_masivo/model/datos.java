package gob.mdmq.mdmq_cmi_masivo.model;


import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Data;


@Data
public class datos {
    Map<String, Object> datos = new LinkedHashMap<>();
    
        
    @JsonAnyGetter
    public Map<String, Object> getLogs(){
        return datos;
    }
    
    @JsonAnySetter
    public void setLogs(String Key, Object value){
        datos.put(Key, value);
    }

}
