package com.subjectstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.subjectstore.model.FeedRequest;
import com.subjectstore.model.ViewRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
public class SubjectControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    public void testFeedEndpoint_shouldAcceptSubjectData() throws Exception {
        FeedRequest request = new FeedRequest();
        request.id = "building_001";
        request.type = "building"; // CAMBIADO de "financial" → "building"
        request.source = "sensor-1";
        request.timestamp = "2025-01-15T12:00:00Z";
        request.attributes = Map.of(
                "temperature", 23.5,
                "status", "active",
                "visitors", 150
        );

        mockMvc.perform(post("/subject/feed")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk());
    }

    @Test
    public void testViewEndpoint_shouldReturnProcessedData() throws Exception {
        testFeedEndpoint_shouldAcceptSubjectData();

        ViewRequest request = new ViewRequest();
        request.id = "building_001";
        request.type = "building";
        request.yaml = """
            rows:
              from: 2025-01
              to: 2025-12
              period: P1Y
            columns:
              - name: "Temp"
                calc: "temperature.average"
              - name: "Visitors"
                calc: "visitors.count"
            """;

        MvcResult result = mockMvc.perform(post("/subject/view")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andReturn();

        String response = result.getResponse().getContentAsString();
        System.out.println("Response body:\n" + response);

        assertThat(response).contains("Temp", "Visitors");
    }
}
