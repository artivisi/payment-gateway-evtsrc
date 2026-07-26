package com.artivisi.paymentgateway.web.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
    "spring.datasource.url=jdbc:h2:mem:testdb_inquiry;DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
    "spring.datasource.driver-class-name=org.h2.Driver",
    "spring.datasource.username=sa",
    "spring.datasource.password=",
    "spring.jpa.hibernate.ddl-auto=create-drop",
    "spring.flyway.enabled=false",
    "spring.kafka.streams.auto-startup=false"
})
@AutoConfigureMockMvc
class InquiryControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("NEGATIVE: Account inquiry with blank bankCode returns HTTP 400 Bad Request")
    void testInquireAccountMissingBankCode_Negative() throws Exception {
        InquiryController.AccountInquiryRequest request = new InquiryController.AccountInquiryRequest("", "88019283019");

        mockMvc.perform(post("/api/inquiry")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("NEGATIVE: Account inquiry with blank vaNumber returns HTTP 400 Bad Request")
    void testInquireAccountMissingVaNumber_Negative() throws Exception {
        InquiryController.AccountInquiryRequest request = new InquiryController.AccountInquiryRequest("MAYBANK", "");

        mockMvc.perform(post("/api/inquiry")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("OUTAGE FAULT TOLERANCE: Inquiry when streams engine is starting up returns 503 Service Unavailable cleanly")
    void testInquireAccountStreamsUnavailable_Graceful() throws Exception {
        InquiryController.AccountInquiryRequest request = new InquiryController.AccountInquiryRequest("MAYBANK", "88019283019");

        mockMvc.perform(post("/api/inquiry")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value("ERROR"));
    }
}
