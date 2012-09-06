package com.jayway.hadoop.fress;

import org.apache.hadoop.io.Text;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class JsonUtil {

    static ObjectMapper mapper = new ObjectMapper();

    public static JsonNode fromText(Text text){
        JsonNode node = null;
        try {
            node = mapper.readValue(text.getBytes(), JsonNode.class);
            return node;
        } catch (RuntimeException e) {
            System.out.println(e.getMessage());
            return null;
        } catch (JsonMappingException e) {
            System.out.println(e.getMessage());
            return null;
        } catch (JsonParseException e) {
            System.out.println(e.getMessage());
            return null;
        } catch (IOException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }
}
