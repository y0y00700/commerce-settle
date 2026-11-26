package com.commercesettle.settlement.presentation.controller;

import com.commercesettle.settlement.application.port.DailyBatchTrigger;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.time.LocalDate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/** 성능 측정 전용. {@code settlement.perf.trigger-enabled=true}일 때만 등록된다. */
@RestController
@ConditionalOnProperty(prefix = "settlement.perf", name = "trigger-enabled", havingValue = "true")
public class PerfDailySettlementController {
  private final DailyBatchTrigger trigger;

  public PerfDailySettlementController(DailyBatchTrigger trigger) {
    this.trigger = trigger;
  }

  public record RunRequest(@NotNull LocalDate batchDate) {}

  public record RunAccepted(LocalDate batchDate) {}

  @Operation(operationId = "post_internal_perf_daily_settlement_runs", summary = "측정용 일배치 시작")
  @PostMapping("/internal/perf/daily-settlement-runs")
  @ResponseStatus(HttpStatus.ACCEPTED)
  public RunAccepted start(@Valid @RequestBody RunRequest request) {
    trigger.start(request.batchDate());
    return new RunAccepted(request.batchDate());
  }
}
