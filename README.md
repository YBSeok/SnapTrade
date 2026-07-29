# Snap Trade

Snap Trade는 처리량과 지연 시간 최적화에 주력하는 암호화폐 거래소 플랫폼입니다. 다른 사항보다도 자체적인 성능을 최우선 순위로 두고, 상용화 가능성에 대해 검토합니다.

기능 개선의 가장 큰 지표는 동시 접속자 수이며, C10K부터 시작해서 C100K까지 늘려가며 검증과 개선을 단계적으로 진행합니다.

## 핵심 기능

포함된 대표적인 기능은 아래와 같습니다.

* **주문 및 매칭 (Order & Matching)**
  사용자의 매수 및 매도 주문을 실시간으로 접수하고 관리하며, 시장 상황에 따라 주문을 체결시키는 거래소의 핵심 엔진 기능입니다.
* **계정 및 원장 (Account & Ledger)**
  사용자의 가상 자산 및 법정화폐 잔고를 관리하고, 매칭 엔진에서 발생한 거래 체결 내역을 기록하여 자산의 이동과 보유 상태를 정확하게 증명하는 무결성 보장 기능입니다.
* **시장 데이터 및 스트리밍 (Market Data & Streaming)**
  주문 및 체결 과정을 통해 지속적으로 변화하는 실시간 호가(Orderbook), 체결 내역(Trade), 시세(Ticker) 및 차트용 시계열 데이터(Kline)를 가공하여 모든 사용자에게 지연 없이 전파하는 기능입니다.

## Design Documents

아키텍처와 내부 동작은 [`design/`](design/README.md) 문서를 참고하세요.

| Document | Description |
| :--- | :--- |
| Design Documents | Description |
| :--- | :--- |
| [Matching Engine](design/matching_engine.md) | Order intake, Disruptor pipeline (Matching → Journal → Publisher), Kafka fan-out, status polling |
| [Ledger](design/ledger.md) | Account balances and append-only ledger for trades, deposits, and withdrawals |
| [Data Streaming](design/data_streaming.md) | Public ticker / kline STOMP streams driven by `trade.completed` |
| [Notification](design/notification.md) | Private per-user alerts on `/user/queue/notifications` |

Kafka broker는 `docker compose up -d kafka` 로 띄운 뒤 앱을 실행하세요 (`localhost:9092`).

## 성능 목표

| 지표 | 목표값 |
| :--- | :--- |
| **매칭 엔진 처리량** | 50,000 TPS |
| **주문 처리 지연 (P99)** | < 1ms |
| **동시 접속 유저 수** | C100K |
| **실시간 메시지 지연** | < 50ms |
| **REST API 응답 지연 (P95)** | < 50ms |
| **Orderbook 업데이트 주기** | < 10ms |

---

## Core Matching Engine 성능 최적화 내역

코어 매칭 엔진은 플랫폼의 심장부로서, 초당 수만 건의 트랜잭션을 지연 없이 처리하기 위해 병목 지점을 추적하고 아키텍처를 근본적으로 재설계했습니다. 주요 성능 개선 과정은 PAAR (Problem, Analyze, Action, Result) 구조로 기술합니다.

### 1. 인메모리 격리 및 이벤트 소싱 (Event Sourcing) 아키텍처 도입

* **[P] RDBMS 동기화로 인한 I/O 병목 및 백프레셔(Backpressure) 역류**
  주문 접수 및 체결 시 매칭 연산(Compute)과 DB 트랜잭션(I/O)이 결합되어 동작. DB I/O의 지연이 전체 매칭 큐의 백프레셔를 유발하여 목표 처리량 달성 불가. 장애 발생 시 메모리 내 호가창(OrderBook) 데이터 증발 위험 존재.
* **[A] 결정론적 상태 머신(Deterministic State Machine)으로의 격리 필요성**
  매칭 엔진(Compute-bound)과 영속성 계층(I/O-bound)의 물리적 격리 필요성 도출. 외부 I/O 의존성 없이 입력된 주문에 의해서만 상태가 변하는 고립된 환경 구성 분석.
* **[A] 이벤트 소싱 아키텍처(Event Sourcing) 전면 도입**
  1. 매칭 엔진을 순수 메모리 연산으로 격리하고, 체결 결과를 순차적 이벤트 스트림(Journal)으로 비동기 발행.
  2. 비즈니스 로직 처리(Command)와 데이터 조회(Query/Projection) 파이프라인 분리.
  3. 서버 부팅 시 영속화된 이벤트를 시간순으로 리플레이(Replay)하여 1초 이내에 인메모리 호가창 상태를 완벽히 복구하는 로직 구현.
* **[R] 매칭 로직의 분리 및 지연 시간 보장**
  DB 트랜잭션 지연으로부터 코어 매칭 로직의 완벽한 분리(Decoupling) 달성. 데이터 유실(Data Loss) 없이 매칭 엔진의 순수 연산 속도를 보장하여 **목표 P99 지연 시간(< 1ms) 달성의 아키텍처적 기반 마련.**

### 2. Zero-Allocation 최적화 (`BigDecimal -> long` 전환)

* **[P] 객체 할당으로 인한 심각한 GC 오버헤드 및 Allocation Stall 발생**
  정밀한 금융 계산을 위해 `BigDecimal`을 사용한 결과, 50,000 TPS 환경에서 초당 수십만 개의 힙(Heap) 메모리 객체가 생성 및 폐기됨. 지속적인 가비지 컬렉션(Minor/Major GC) 개입으로 인해 CPU 사이클이 낭비되고, 매칭 지연 시간의 불규칙한 튐(Jitter) 현상 발생.
* **[A] 객체 생성 비용 제로화 분석**
  금융 도메인의 소수점 정밀도를 무손실로 유지하면서도 동시성 환경에서 힙 메모리 할당을 원천 차단해야 함. 극단적인 처리량을 위해 핵심 매칭 루프 내의 객체 생성(Object Instantiation) 비용을 0으로 만들어야 한다고 분석.
* **[A] 원시 타입(Primitive Type) 스케일링을 통한 Zero-Allocation 구현**
  1. 모든 호가 및 수량 데이터를 특정 소수점 틱(Tick) 사이즈 기준의 `long` 타입 정수형으로 전면 전환.
  2. 코어 매칭 엔진 내부의 모든 체결 로직 및 내부 변수 상태 갱신 과정에서 `new` 키워드 사용을 완전히 제거.
* **[R] GC 오버헤드 제로 달성**
  핵심 매칭 루프 구간의 **객체 할당 및 GC 오버헤드 제로(0%) 달성.** 메모리 파편화 현상을 제거하고 캐시 지역성(Cache Locality)을 확보하여, 단일 체결 연산 시간을 마이크로초 단위에서 나노초 단위로 단축.

### 3. 무잠금(Lock-Free) 아키텍처 도입 (LMAX Disruptor)

* **[P] 락 경합(Lock Contention)에 따른 커널 레벨 컨텍스트 스위칭 폭증**
  다중 스레드 환경에서 `LinkedBlockingQueue`를 사용할 때 발생하는 `ReentrantLock` 경합으로 인해 톰캣 워커 스레드들의 강제 수면(`park`) 및 기상(`unpark`)이 빈번하게 발생. PID 모니터링 결과 자발적 컨텍스트 스위칭(cswch/s) 수치가 초당 수십만 건으로 폭증하며 CPU 자원 고갈.
* **[A] 하드웨어 친화적 아키텍처 필요성 도출**
  다중 스레드 락(Lock) 구조의 태생적 한계 및 큐 간 데이터 이동에 따른 하드웨어 캐시 미스(Cache Miss) 문제 도출. OS 스케줄러의 개입을 배제하는 하드웨어 및 캐시 친화적(Mechanical Sympathy) 파이프라인 설계 필요성 대두.
* **[A] 단일 링 버퍼(Ring Buffer) 기반의 LMAX Disruptor 파이프라인 구축**
  1. 크기 65,536의 사전 할당된 단일 링 버퍼 적용으로 락(Lock) 없는 CAS 명령어 기반 시퀀스 할당 구현.
  2. 캐시 일관성 무효화 현상인 거짓 공유(False Sharing)를 방지하기 위해 캐시 라인 패딩(Padding) 삽입.
  3. 의존성 배리어(Sequence Barrier)를 통한 매칭 -> 저널 -> 프로젝션 순차 파이프라인 구성 및 하위 I/O 병목 해소를 위한 스마트 배칭(Smart Batching) 적용.
* **[R] 스루풋 선형 확장 및 컨텍스트 스위칭 오버헤드 소멸**
  OS 커널의 컨텍스트 스위칭 비용 완벽 제거. **큐잉 레이턴시(Queuing Latency) 제로화 및 시스템 전체의 스루풋 선형 확장(Linear Scaling) 달성**을 통해 목표치인 C100K 환경에서의 안정적인 동시성 처리 한계 돌파.
