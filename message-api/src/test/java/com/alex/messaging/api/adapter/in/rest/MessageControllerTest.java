package com.alex.messaging.api.adapter.in.rest;

import com.alex.messaging.api.application.MessageAccepted;
import com.alex.messaging.api.application.MessageService;
import com.alex.messaging.api.application.MessageView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * QA pass, Phase 2: REST-edge validation and error-shape behavior. {@code messageService} is
 * mocked so no Kafka broker is needed — this only exercises MessageController + Bean
 * Validation + ApiExceptionHandler.
 */
@WebMvcTest(MessageController.class)
class MessageControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private MessageService messageService;

    private void stubHappyPath() {
        when(messageService.create(anyInt(), anyString(), any()))
                .thenReturn(new MessageAccepted(UUID.randomUUID(), 1, "corr-id"));
        when(messageService.update(anyInt(), anyString(), any()))
                .thenReturn(new MessageAccepted(UUID.randomUUID(), 1, "corr-id"));
        when(messageService.read(anyInt(), any()))
                .thenReturn(new MessageView(1, "hello"));
    }

    // ---- id edge cases (Create: id in JSON body) ----

    @Test
    void createWithIdZero_isRejected() throws Exception {
        mockMvc.perform(post("/api/v1/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"id": 0, "msg": "hello"}"""))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));
    }

    @Test
    void createWithNegativeId_isRejected() throws Exception {
        mockMvc.perform(post("/api/v1/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"id": -1, "msg": "hello"}"""))
                .andExpect(status().isBadRequest());
    }

    @Test
    void createWithIntMaxValueId_isAccepted() throws Exception {
        stubHappyPath();
        mockMvc.perform(post("/api/v1/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"id": 2147483647, "msg": "hello"}"""))
                .andExpect(status().isAccepted());
    }

    @Test
    void createWithIdAsNumericJsonString_isAcceptedByDesign() throws Exception {
        // Deliberate leniency, not an oversight: "5" and 5 carry the same value, no
        // information lost, so Jackson's default scalar coercion is left enabled here.
        stubHappyPath();
        mockMvc.perform(post("/api/v1/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"id": "5", "msg": "hello"}"""))
                .andExpect(status().isAccepted());
    }

    @Test
    void createWithIdAsNonNumericString_isRejected() throws Exception {
        // The line: a string is fine if it actually represents a number - anything else
        // must still fail, same as malformed JSON would.
        mockMvc.perform(post("/api/v1/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"id": "abc", "msg": "hello"}"""))
                .andExpect(status().isBadRequest());
    }

    @Test
    void createWithIdAsDecimal_isRejectedNotSilentlyTruncated() throws Exception {
        // spring.jackson.deserialization.accept-float-as-int: false - 5.7 would otherwise
        // be silently truncated to 5, a real precision-loss bug, not harmless leniency.
        mockMvc.perform(post("/api/v1/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"id": 5.7, "msg": "hello"}"""))
                .andExpect(status().isBadRequest());
    }

    // ---- malformed request shapes: every one of these must be a client error (4xx),
    // never a bare 500. See ApiExceptionHandler extending ResponseEntityExceptionHandler. ----

    private record MalformedRequest(String description, HttpMethod method, String path,
                                     MediaType contentType, String body, int expectedStatus) {
        @Override
        public String toString() {
            return description;
        }
    }

    private static Stream<MalformedRequest> malformedRequests() {
        MediaType json = MediaType.APPLICATION_JSON;
        return Stream.of(
                new MalformedRequest("id overflowing int range", HttpMethod.POST, "/api/v1/messages",
                        json, "{\"id\": 99999999999999, \"msg\": \"hello\"}", 400),
                new MalformedRequest("id missing entirely", HttpMethod.POST, "/api/v1/messages",
                        json, "{\"msg\": \"hello\"}", 400),
                new MalformedRequest("malformed JSON syntax", HttpMethod.POST, "/api/v1/messages",
                        json, "{\"id\": 1, \"msg\": ", 400),
                new MalformedRequest("empty body", HttpMethod.POST, "/api/v1/messages",
                        json, "", 400),
                new MalformedRequest("non-numeric path id", HttpMethod.PUT, "/api/v1/messages/abc",
                        json, "{\"msg\": \"hello\"}", 400),
                new MalformedRequest("wrong content type", HttpMethod.POST, "/api/v1/messages",
                        MediaType.TEXT_PLAIN, "{\"id\": 1, \"msg\": \"hello\"}", 415),
                new MalformedRequest("wrong http method", HttpMethod.PATCH, "/api/v1/messages/1",
                        json, "{\"msg\": \"hello\"}", 405));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedRequests")
    void malformedRequestsReturnClientErrorNotServerError(MalformedRequest testCase) throws Exception {
        mockMvc.perform(request(testCase.method(), testCase.path())
                        .contentType(testCase.contentType())
                        .content(testCase.body()))
                .andExpect(status().is(testCase.expectedStatus()));
    }

    @Test
    void deleteWithZeroPathId_isRejected() throws Exception {
        mockMvc.perform(delete("/api/v1/messages/0"))
                .andExpect(status().isBadRequest());
    }

    // ---- msg edge cases ----

    @Test
    void createWithNullMsg_isRejected() throws Exception {
        mockMvc.perform(post("/api/v1/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"id": 1, "msg": null}"""))
                .andExpect(status().isBadRequest());
    }

    @Test
    void createWithEmptyMsg_isRejected() throws Exception {
        mockMvc.perform(post("/api/v1/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"id": 1, "msg": ""}"""))
                .andExpect(status().isBadRequest());
    }

    @Test
    void createWithWhitespaceOnlyMsg_isRejected() throws Exception {
        mockMvc.perform(post("/api/v1/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"id": 1, "msg": "   "}"""))
                .andExpect(status().isBadRequest());
    }

    @Test
    void createWithMsgAtExactMaxSize_isAccepted() throws Exception {
        stubHappyPath();
        String msg = "a".repeat(1000);
        mockMvc.perform(post("/api/v1/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\": 1, \"msg\": \"" + msg + "\"}"))
                .andExpect(status().isAccepted());
    }

    @Test
    void createWithMsgOneOverMaxSize_isRejected() throws Exception {
        String msg = "a".repeat(1001);
        mockMvc.perform(post("/api/v1/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\": 1, \"msg\": \"" + msg + "\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void createWithEmojiMsgAtExactMaxUtf16Length_isAccepted() throws Exception {
        // Astral-plane emoji are 2 UTF-16 code units each - @Size counts String.length()
        // (UTF-16 units), so 500 emoji = 1000 units = exactly at the limit.
        stubHappyPath();
        String msg = "😀".repeat(500); // 😀 x500
        if (msg.length() != 1000) {
            throw new IllegalStateException("test setup assumption broken: length=" + msg.length());
        }
        mockMvc.perform(post("/api/v1/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\": 1, \"msg\": \"" + msg + "\"}"))
                .andExpect(status().isAccepted());
    }

    @Test
    void createWithVeryLargeMsg_isRejectedNotUnhandled() throws Exception {
        // 50k chars, well past @Size(max=1000) - confirms oversized payloads are still
        // rejected cleanly via validation, not by an unhandled exception/timeout.
        String msg = "x".repeat(50_000);
        mockMvc.perform(post("/api/v1/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\": 1, \"msg\": \"" + msg + "\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void unknownExtraFields_areIgnored() throws Exception {
        stubHappyPath();
        mockMvc.perform(post("/api/v1/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"id": 1, "msg": "hello", "bogusField": "whatever"}"""))
                .andExpect(status().isAccepted());
    }

    // ---- ProblemDetail shape sanity check on a known-good 400 ----

    @Test
    void validationFailure_returnsProblemDetailShapeWithNoLeakage() throws Exception {
        mockMvc.perform(post("/api/v1/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"id": 0, "msg": ""}"""))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.title").exists())
                .andExpect(jsonPath("$.detail").exists())
                .andExpect(jsonPath("$.detail", org.hamcrest.Matchers.not(
                        org.hamcrest.Matchers.containsString("com.alex.messaging"))))
                .andExpect(jsonPath("$.detail", org.hamcrest.Matchers.not(
                        org.hamcrest.Matchers.containsString("Exception"))));
    }

    @Test
    void readOfMissingMessage_maps404NotFound() throws Exception {
        when(messageService.read(anyInt(), any()))
                .thenThrow(new com.alex.messaging.api.domain.exception.MessageNotFoundException(999));
        mockMvc.perform(get("/api/v1/messages/999"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404));
    }
}
