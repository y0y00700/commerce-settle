package com.commercesettle.settlement.infrastructure.batch;

import com.commercesettle.settlement.application.port.DailyBatchTrigger;
import com.commercesettle.settlement.domain.model.SettlementBatch;
import java.time.Clock;
import java.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

/**
 * 성능 측정 전용 일배치 시작. 정기 스케줄과 같은 {@link DailySettlementLauncher#run} 경로를 쓰므로 실행자 잠금·재시작·완료 판정이 같다.
 * 기본으로 꺼져 있으며 측정 환경에서만 켠다.
 */
@Component
@ConditionalOnProperty(prefix = "settlement.perf", name = "trigger-enabled", havingValue = "true")
class PerfDailyBatchTrigger implements DailyBatchTrigger {
  private static final Logger LOGGER = LoggerFactory.getLogger(PerfDailyBatchTrigger.class);

  private final DailySettlementLauncher launcher;
  private final Clock clock;
  private final TaskExecutor executor;

  @Autowired
  PerfDailyBatchTrigger(DailySettlementLauncher launcher, Clock clock) {
    this(launcher, clock, new SimpleAsyncTaskExecutor("perf-daily-settlement-"));
  }

  PerfDailyBatchTrigger(DailySettlementLauncher launcher, Clock clock, TaskExecutor executor) {
    this.launcher = launcher;
    this.clock = clock;
    this.executor = executor;
  }

  @Override
  public void start(LocalDate batchDate) {
    // 실행 스레드에서 거절되면 호출자가 알 수 없으므로 마감 확인은 접수 전에 한다.
    if (SettlementBatch.cutoffOf(batchDate).isAfter(clock.instant())) {
      throw new IllegalArgumentException("cutoff of batchDate has not passed yet");
    }
    executor.execute(
        () -> {
          try {
            var status = launcher.run(batchDate);
            LOGGER
                .atInfo()
                .addKeyValue("event", "settlement.perf-trigger")
                .addKeyValue("batchDate", batchDate)
                .addKeyValue("result", status)
                .log("Perf-triggered daily settlement finished");
          } catch (RuntimeException e) {
            LOGGER
                .atWarn()
                .setCause(e)
                .addKeyValue("event", "settlement.perf-trigger")
                .addKeyValue("batchDate", batchDate)
                .log("Perf-triggered daily settlement failed");
          }
        });
  }
}
