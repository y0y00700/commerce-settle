package com.commercesettle.settlement.infrastructure.batch;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.commercesettle.settlement.domain.model.SettlementBatch;
import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PerfDailyBatchTriggerTest {
  private static final LocalDate BATCH_DATE = LocalDate.of(2026, 11, 1);

  private final DailySettlementLauncher launcher = mock(DailySettlementLauncher.class);

  @Test
  @DisplayName("마감이 지난 batchDate는 정기 일배치와 같은 실행 경로로 시작한다")
  void startsDueBatchThroughLauncher() {
    var trigger = new PerfDailyBatchTrigger(launcher, clockAt(1), Runnable::run);

    trigger.start(BATCH_DATE);

    verify(launcher).run(BATCH_DATE);
  }

  @Test
  @DisplayName("마감 전 batchDate는 실행을 접수하지 않고 거절한다")
  void rejectsBatchBeforeCutoff() {
    var trigger = new PerfDailyBatchTrigger(launcher, clockAt(-1), Runnable::run);

    assertThatThrownBy(() -> trigger.start(BATCH_DATE))
        .isInstanceOf(IllegalArgumentException.class);
    verifyNoInteractions(launcher);
  }

  private static Clock clockAt(long secondsFromCutoff) {
    return Clock.fixed(
        SettlementBatch.cutoffOf(BATCH_DATE).plusSeconds(secondsFromCutoff), ZoneOffset.UTC);
  }
}
