package com.commercesettle.settlement.application.port;

import java.time.LocalDate;

/** 성능 측정용으로 정기 일배치를 지정한 batchDate로 즉시 시작한다. 실행은 비동기이며 결과는 Spring Batch 실행 기록에서 확인한다. */
public interface DailyBatchTrigger {
  /**
   * @throws IllegalArgumentException batchDate의 마감이 아직 지나지 않았으면
   */
  void start(LocalDate batchDate);
}
