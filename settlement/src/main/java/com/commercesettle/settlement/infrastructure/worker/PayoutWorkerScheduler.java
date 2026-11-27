package com.commercesettle.settlement.infrastructure.worker;

import com.commercesettle.settlement.application.dto.PayoutWorkerSettings;
import com.commercesettle.settlement.application.service.PayoutWorker;
import com.commercesettle.settlement.domain.repository.PayoutJobRepository.LeaseKind;
import com.commercesettle.settlement.domain.repository.PayoutJobRepository.LeasedJob;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 지급 Worker 디스패처. 정해진 간격마다 빈 슬롯 수만큼만 작업을 임대해 실행 풀에 넘긴다. 주말·공휴일도 같다. 결과 조회·중단 복구를 먼저 빈 슬롯까지 임대하고, 새
 * 지급은 복구 전용 슬롯을 남긴 나머지에서만 임대한다. 임대는 실행 직전에 해서 대기열에서 임대 시간을 쓰지 않는다.
 *
 * <p>작업을 마친 슬롯은 돌려주기 전에 도래한 작업을 이어서 임대해 실행하고, 없을 때만 돌려준다. 폴링과 같은 우선순위로 복구를 먼저 임대하며, 새 지급 슬롯만 복구가 없을
 * 때 새 지급을 이어받는다. 슬롯 수는 그대로라 동시 실행 상한과 새 지급 상한이 유지되고, 다음 폴링까지 슬롯이 비는 틈 없이 응답 시간만큼 처리한다. 종료가 시작되면 이어서
 * 임대하지 않는다.
 *
 * <p>한 작업의 예외는 기록하고 그 슬롯을 돌려준다. 연속 실패가 폴링 주기를 넘어 시도 횟수를 소진하지 않게, 실패한 슬롯은 이어서 임대하지 않는다. 그 작업은 임대 만료
 * 뒤 복구로 다시 임대되고 시도 상한에서 운영 확인으로 넘어간다.
 */
@Component
@ConditionalOnProperty(
    prefix = "settlement.payout-worker",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
class PayoutWorkerScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(PayoutWorkerScheduler.class);

  private final PayoutWorker worker;
  private final Executor executor;
  private final int maxConcurrency;
  private final int newSlots;
  // 늘리는 쪽은 폴링 스레드 하나뿐이라 확인 후 증가가 상한을 넘지 않는다. 줄이는 쪽은 실행 스레드다.
  private final AtomicInteger running = new AtomicInteger();
  private final AtomicInteger runningNew = new AtomicInteger();
  private volatile boolean stopping;
  private ApplicationContext context;

  PayoutWorkerScheduler(
      PayoutWorker worker,
      @Qualifier("payoutWorkerExecutor") Executor executor,
      PayoutWorkerSettings settings) {
    this.worker = worker;
    this.executor = executor;
    this.maxConcurrency = settings.maxConcurrency();
    this.newSlots = settings.maxConcurrency() - settings.recoverySlots();
  }

  @Scheduled(fixedDelayString = "${settlement.payout-worker.poll-interval-ms}")
  void poll() {
    if (stopping) return;
    while (running.get() < maxConcurrency && dispatch(LeaseKind.RECOVERY)) {}
    while (running.get() < maxConcurrency
        && runningNew.get() < newSlots
        && dispatch(LeaseKind.NEW)) {}
  }

  @Autowired
  void applicationContext(ApplicationContext context) {
    this.context = context;
  }

  /**
   * 실행 풀이 실행 중인 작업을 기다리는 동안 새 작업을 이어받지 않도록 종료 시작 시점에 멈춘다. 관리 포트용 자식 컨텍스트의 종료 이벤트도 전파되므로 이 빈의 컨텍스트만
   * 본다.
   */
  @EventListener
  void onContextClosed(ContextClosedEvent event) {
    if (event.getApplicationContext() == context) stop();
  }

  void stop() {
    stopping = true;
  }

  private boolean dispatch(LeaseKind kind) {
    Optional<LeasedJob> leased = lease(kind);
    if (leased.isEmpty()) {
      return false;
    }
    LeasedJob job = leased.get();
    acquire(kind);
    try {
      executor.execute(() -> runAndRelease(job, kind));
    } catch (RejectedExecutionException e) {
      release(kind);
      logFailure(e, "Payout job was not started; it will be recovered after its lease expires");
      return false;
    }
    return true;
  }

  private Optional<LeasedJob> lease(LeaseKind kind) {
    try {
      return worker.lease(kind);
    } catch (RuntimeException e) {
      logFailure(e, "Payout job lease failed; it will be retried at the next poll");
      return Optional.empty();
    }
  }

  private void runAndRelease(LeasedJob first, LeaseKind kind) {
    try {
      Optional<LeasedJob> next = Optional.of(first);
      while (next.isPresent()) {
        try {
          worker.run(next.get());
        } catch (RuntimeException e) {
          logFailure(e, "Payout job failed; it will be retried after its lease expires");
          return;
        }
        next = stopping ? Optional.empty() : leaseNext(kind);
      }
    } finally {
      release(kind);
    }
  }

  /** 폴링과 같이 복구를 먼저 임대한다. 복구 슬롯은 새 지급 상한에 들지 않으므로 복구만 이어받는다. */
  private Optional<LeasedJob> leaseNext(LeaseKind kind) {
    Optional<LeasedJob> recovery = lease(LeaseKind.RECOVERY);
    if (recovery.isPresent() || kind == LeaseKind.RECOVERY) {
      return recovery;
    }
    return lease(LeaseKind.NEW);
  }

  private void acquire(LeaseKind kind) {
    running.incrementAndGet();
    if (kind == LeaseKind.NEW) runningNew.incrementAndGet();
  }

  private void release(LeaseKind kind) {
    if (kind == LeaseKind.NEW) runningNew.decrementAndGet();
    running.decrementAndGet();
  }

  private static void logFailure(RuntimeException e, String message) {
    LOGGER
        .atError()
        .setCause(e)
        .addKeyValue("event", "settlement.payout")
        .addKeyValue("result", "ERROR")
        .addKeyValue("errorType", e.getClass().getName())
        .log(message);
  }
}
