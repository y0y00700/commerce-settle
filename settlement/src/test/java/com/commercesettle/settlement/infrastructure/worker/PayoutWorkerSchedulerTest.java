package com.commercesettle.settlement.infrastructure.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.commercesettle.settlement.application.dto.PayoutWorkerSettings;
import com.commercesettle.settlement.application.service.PayoutWorker;
import com.commercesettle.settlement.domain.repository.PayoutJobRepository.LeaseKind;
import com.commercesettle.settlement.domain.repository.PayoutJobRepository.LeasedJob;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 디스패처의 슬롯 계수. 실행은 붙잡을 수 있는 대역 Worker로 바꾼다. */
class PayoutWorkerSchedulerTest {
  private static final PayoutWorkerSettings SETTINGS =
      new PayoutWorkerSettings(
          Duration.ofSeconds(30), Duration.ofSeconds(5), Duration.ofMinutes(5), 10, 8, 2);

  private final Map<LeaseKind, Queue<LeasedJob>> due =
      Map.of(
          LeaseKind.NEW,
          new ConcurrentLinkedQueue<>(),
          LeaseKind.RECOVERY,
          new ConcurrentLinkedQueue<>());
  private final Set<UUID> failing = ConcurrentHashMap.newKeySet();
  private final AtomicInteger runningNew = new AtomicInteger();
  private final AtomicInteger runningRecovery = new AtomicInteger();
  private final AtomicInteger finished = new AtomicInteger();
  private final AtomicInteger maxNew = new AtomicInteger();
  private final AtomicInteger runningTotal = new AtomicInteger();
  private final AtomicInteger maxTotal = new AtomicInteger();
  private final AtomicInteger recoveryFinished = new AtomicInteger();
  private final CountDownLatch hold = new CountDownLatch(1);
  private volatile long workMillis;
  private volatile CountDownLatch failGate = new CountDownLatch(0);
  private final ExecutorService pool = Executors.newCachedThreadPool();
  private PayoutWorker worker;

  @BeforeEach
  void stubWorker() throws InterruptedException {
    worker = mock(PayoutWorker.class);
    when(worker.lease(any()))
        .thenAnswer(call -> Optional.ofNullable(due.get(call.<LeaseKind>getArgument(0)).poll()));
    doAnswer(
            call -> {
              LeasedJob job = call.getArgument(0);
              AtomicInteger counter = job.attempts() == 0 ? runningNew : runningRecovery;
              counter.incrementAndGet();
              maxNew.accumulateAndGet(runningNew.get(), Math::max);
              maxTotal.accumulateAndGet(runningTotal.incrementAndGet(), Math::max);
              try {
                if (failing.contains(job.jobId())) {
                  failGate.await();
                  throw new IllegalStateException("injected");
                }
                hold.await();
                TimeUnit.MILLISECONDS.sleep(workMillis);
              } finally {
                runningTotal.decrementAndGet();
                counter.decrementAndGet();
                if (counter == runningRecovery) recoveryFinished.incrementAndGet();
                finished.incrementAndGet();
              }
              return null;
            })
        .when(worker)
        .run(any());
  }

  @AfterEach
  void stopPool() {
    hold.countDown();
    pool.shutdownNow();
  }

  @Test
  @DisplayName("새 지급은 복구 전용 슬롯을 남긴 6건까지만 실행하고, 다음 폴링에도 더 임대하지 않는다")
  void newPayoutsLeaveRecoverySlots() throws InterruptedException {
    addDue(LeaseKind.NEW, 12);
    PayoutWorkerScheduler scheduler = new PayoutWorkerScheduler(worker, pool, SETTINGS);

    scheduler.poll();
    awaitRunning(runningNew, 6);
    scheduler.poll();

    assertThat(runningNew).hasValue(6);
    assertThat(due.get(LeaseKind.NEW)).hasSize(6);
  }

  @Test
  @DisplayName("새 지급 6건이 실행 중이어도 복구 작업은 다음 폴링에서 전용 슬롯 2개로 바로 실행하고, 합계는 8을 넘지 않는다")
  void recoveryUsesReservedSlots() throws InterruptedException {
    addDue(LeaseKind.NEW, 6);
    PayoutWorkerScheduler scheduler = new PayoutWorkerScheduler(worker, pool, SETTINGS);
    scheduler.poll();
    awaitRunning(runningNew, 6);
    addDue(LeaseKind.RECOVERY, 3);

    scheduler.poll();
    awaitRunning(runningRecovery, 2);

    assertThat(runningNew.get() + runningRecovery.get()).isEqualTo(8);
    assertThat(due.get(LeaseKind.RECOVERY)).hasSize(1);
  }

  @Test
  @DisplayName("새 지급이 없으면 복구 작업은 일반 슬롯까지 8건을 쓴다")
  void recoveryMayUseAllSlots() throws InterruptedException {
    addDue(LeaseKind.RECOVERY, 10);
    PayoutWorkerScheduler scheduler = new PayoutWorkerScheduler(worker, pool, SETTINGS);

    scheduler.poll();
    awaitRunning(runningRecovery, 8);

    assertThat(due.get(LeaseKind.RECOVERY)).hasSize(2);
  }

  @Test
  @DisplayName("실행 중 예외가 난 작업도 슬롯을 돌려주어 다음 폴링에서 다른 작업이 실행된다")
  void failedJobReleasesSlot() throws InterruptedException {
    for (int i = 0; i < 6; i++) failing.add(addDue(LeaseKind.NEW, 1));
    PayoutWorkerScheduler scheduler = new PayoutWorkerScheduler(worker, pool, SETTINGS);
    scheduler.poll();
    awaitFinished(6);
    addDue(LeaseKind.NEW, 6);

    // 대역 Worker가 끝난 뒤 디스패처가 슬롯을 돌려주기까지 틈이 있어, 실제 주기처럼 다시 폴링한다.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (runningNew.get() < 6) {
      assertThat(System.nanoTime()).as("슬롯이 반납되지 않았다").isLessThan(deadline);
      scheduler.poll();
      TimeUnit.MILLISECONDS.sleep(10);
    }

    awaitRunning(runningNew, 6);
  }

  @Test
  @DisplayName("임대 오류나 실행 풀 거절에도 폴링은 예외 없이 끝나고 거절한 슬롯을 돌려준다")
  void leaseErrorAndRejectionDoNotBreakPolling() throws InterruptedException {
    PayoutWorker broken = mock(PayoutWorker.class);
    when(broken.lease(any())).thenThrow(new IllegalStateException("db down"));
    assertThatCode(new PayoutWorkerScheduler(broken, pool, SETTINGS)::poll)
        .doesNotThrowAnyException();

    addDue(LeaseKind.NEW, 1);
    PayoutWorkerScheduler rejecting =
        new PayoutWorkerScheduler(
            worker,
            task -> {
              throw new RejectedExecutionException("shutting down");
            },
            SETTINGS);
    assertThatCode(rejecting::poll).doesNotThrowAnyException();

    addDue(LeaseKind.NEW, 8);
    PayoutWorkerScheduler scheduler = new PayoutWorkerScheduler(worker, pool, SETTINGS);
    scheduler.poll();
    awaitRunning(runningNew, 6);
  }

  @Test
  @DisplayName("작업을 마친 슬롯은 다음 폴링을 기다리지 않고 도래한 새 지급을 이어서 실행한다")
  void finishedSlotContinuesWithoutNextPoll() throws InterruptedException {
    addDue(LeaseKind.NEW, 60);
    hold.countDown();
    workMillis = 2;
    PayoutWorkerScheduler scheduler = new PayoutWorkerScheduler(worker, pool, SETTINGS);

    scheduler.poll();
    awaitFinished(60);

    assertThat(due.get(LeaseKind.NEW)).isEmpty();
    assertThat(maxNew.get()).isLessThanOrEqualTo(6);
  }

  @Test
  @DisplayName("이어서 실행해도 새 지급은 6건, 전체는 8건을 넘지 않는다")
  void continuationKeepsSlotLimits() throws InterruptedException {
    addDue(LeaseKind.NEW, 80);
    hold.countDown();
    workMillis = 2;
    PayoutWorkerScheduler scheduler = new PayoutWorkerScheduler(worker, pool, SETTINGS);
    scheduler.poll();
    // 새 지급이 이어서 도는 중에 복구가 도래해 두 종류가 섞여 실행되게 한다.
    awaitFinished(6);
    addDue(LeaseKind.RECOVERY, 80);

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (finished.get() < 160) {
      assertThat(System.nanoTime()).as("작업이 끝나지 않았다").isLessThan(deadline);
      scheduler.poll();
      TimeUnit.MILLISECONDS.sleep(5);
    }

    assertThat(maxNew.get()).isLessThanOrEqualTo(6);
    assertThat(maxTotal.get()).isLessThanOrEqualTo(8);
  }

  @Test
  @DisplayName("새 지급 적체가 남아 있어도 작업을 마친 슬롯은 도래한 복구를 먼저 이어받는다")
  void continuationPrefersRecovery() throws InterruptedException {
    // 적체가 테스트 스레드 지연 동안 다 빠지지 않을 만큼 넉넉히 둔다(슬롯 6개 × 2ms로 약 1초).
    addDue(LeaseKind.NEW, 3000);
    hold.countDown();
    workMillis = 2;
    PayoutWorkerScheduler scheduler = new PayoutWorkerScheduler(worker, pool, SETTINGS);
    scheduler.poll();
    awaitFinished(12);

    addDue(LeaseKind.RECOVERY, 30);
    awaitValue(recoveryFinished, 30);

    // 폴링 없이 새 지급 슬롯이 복구를 먼저 받았으므로 새 지급 적체가 다 빠지기 전에 복구가 끝난다.
    assertThat(due.get(LeaseKind.NEW)).isNotEmpty();
  }

  @Test
  @DisplayName("실행이 예외로 끝난 슬롯은 남은 작업을 이어서 임대하지 않는다")
  void failedSlotDoesNotContinue() throws InterruptedException {
    for (int i = 0; i < 6; i++) failing.add(addDue(LeaseKind.NEW, 1));
    addDue(LeaseKind.NEW, 6);
    failGate = new CountDownLatch(1);
    PayoutWorkerScheduler scheduler = new PayoutWorkerScheduler(worker, pool, SETTINGS);

    // 폴링이 끝난 뒤에 실패시켜, 남은 작업을 가져갈 수 있는 경로가 이어받기뿐이게 한다.
    scheduler.poll();
    failGate.countDown();
    awaitFinished(6);
    TimeUnit.MILLISECONDS.sleep(100);

    assertThat(finished).hasValue(6);
    assertThat(due.get(LeaseKind.NEW)).hasSize(6);
  }

  @Test
  @DisplayName("종료 중에는 실행 중인 작업만 마치고 다음 작업을 이어서 임대하지 않는다")
  void stoppingDoesNotContinue() throws InterruptedException {
    addDue(LeaseKind.NEW, 12);
    PayoutWorkerScheduler scheduler = new PayoutWorkerScheduler(worker, pool, SETTINGS);
    scheduler.poll();
    awaitRunning(runningNew, 6);

    scheduler.stop();
    hold.countDown();
    awaitFinished(6);
    TimeUnit.MILLISECONDS.sleep(100);

    assertThat(finished).hasValue(6);
    assertThat(due.get(LeaseKind.NEW)).hasSize(6);
  }

  /** 마지막에 넣은 작업 ID를 돌려준다. 새 지급은 시도 0, 복구는 시도 1로 구분한다. */
  private UUID addDue(LeaseKind kind, int count) {
    UUID last = null;
    for (int i = 0; i < count; i++) {
      last = UUID.randomUUID();
      due.get(kind)
          .add(
              new LeasedJob(
                  last,
                  UUID.randomUUID(),
                  UUID.randomUUID(),
                  UUID.randomUUID(),
                  kind == LeaseKind.NEW ? 0 : 1));
    }
    return last;
  }

  private static void awaitRunning(AtomicInteger counter, int expected)
      throws InterruptedException {
    awaitValue(counter, expected);
    // 더 늘어나지 않는지 잠깐 지켜본다.
    TimeUnit.MILLISECONDS.sleep(100);
    assertThat(counter).hasValue(expected);
  }

  private void awaitFinished(int expected) throws InterruptedException {
    awaitValue(finished, expected);
  }

  private static void awaitValue(AtomicInteger counter, int expected) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (counter.get() < expected) {
      assertThat(System.nanoTime())
          .as("expected %d, was %d", expected, counter.get())
          .isLessThan(deadline);
      TimeUnit.MILLISECONDS.sleep(10);
    }
  }
}
