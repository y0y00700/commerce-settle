package com.commercesettle.settlement.presentation.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.commercesettle.settlement.application.port.ProductSettlementContextReader;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** 측정용 일배치 시작 API를 켠 상태에서 빈 구성·운영자 자격 판정·실제 일배치 실행까지 확인한다. */
@SpringBootTest(
    properties = {
      "management.tracing.enabled=false",
      "spring.kafka.admin.auto-create=false",
      "app.internal-auth.senders.gateway.current="
          + PerfDailySettlementControllerIntegrationTest.GATEWAY,
      "app.internal-auth.senders[operator\\:perf].current="
          + PerfDailySettlementControllerIntegrationTest.OPERATOR,
      "app.internal-auth.senders[service\\:commerce].current=test-commerce-retention-credential-0123456789abcdef",
      "settlement.payout.mock-credential=test-settlement-credential-0123456789abcdef0123",
      "settlement.daily-batch.schedule-enabled=false",
      "settlement.payout-worker.enabled=false",
      "settlement.inbox-processor.enabled=false",
      "settlement.perf.trigger-enabled=true"
    })
@AutoConfigureMockMvc
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PerfDailySettlementControllerIntegrationTest {
  static final String GATEWAY = "test-gateway-credential-0123456789abcdef0123";
  static final String OPERATOR = "test-operator-credential-0123456789abcdef0123";
  private static final String PATH = "/internal/perf/daily-settlement-runs";

  @Container @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:18.3").withInitScript("db/settlement.sql");

  @Autowired MockMvc mvc;
  @Autowired JdbcTemplate jdbc;
  @MockitoBean ProductSettlementContextReader contexts;

  @Test
  @DisplayName("운영자 요청은 202로 접수되고 같은 batchDate의 일배치가 완료된다")
  void operatorStartsDailyBatch() throws Exception {
    LocalDate batchDate = LocalDate.now(ZoneId.of("Asia/Seoul")).minusDays(3);

    mvc.perform(request(OPERATOR, batchDate))
        .andExpect(status().isAccepted())
        .andExpect(jsonPath("$.batchDate").value(batchDate.toString()));

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(statuses(batchDate)).containsExactly("COMPLETED"));
  }

  @Test
  @DisplayName("Gateway 자격은 403이고 실행을 남기지 않는다")
  void rejectsGatewayCredential() throws Exception {
    LocalDate batchDate = LocalDate.now(ZoneId.of("Asia/Seoul")).minusDays(4);

    mvc.perform(request(GATEWAY, batchDate)).andExpect(status().isForbidden());

    assertThat(statuses(batchDate)).isEmpty();
  }

  @Test
  @DisplayName("마감 전 batchDate는 400이고 실행을 남기지 않는다")
  void rejectsBatchBeforeCutoff() throws Exception {
    LocalDate batchDate = LocalDate.now(ZoneId.of("Asia/Seoul")).plusDays(1);

    mvc.perform(request(OPERATOR, batchDate)).andExpect(status().isBadRequest());

    assertThat(statuses(batchDate)).isEmpty();
  }

  private static MockHttpServletRequestBuilder request(String credential, LocalDate batchDate) {
    return post(PATH)
        .header(HttpHeaders.AUTHORIZATION, "Bearer " + credential)
        .contentType(MediaType.APPLICATION_JSON)
        .content("{\"batchDate\":\"" + batchDate + "\"}");
  }

  private List<String> statuses(LocalDate batchDate) {
    return jdbc.queryForList(
        "SELECT e.status FROM settlement.batch_job_execution e"
            + " JOIN settlement.batch_job_execution_params p"
            + " ON p.job_execution_id = e.job_execution_id"
            + " WHERE p.parameter_name = 'batchDate' AND p.parameter_value = ?",
        String.class,
        batchDate.toString());
  }
}
