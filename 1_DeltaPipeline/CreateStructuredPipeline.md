# 정형 파이프라인

## 1. 개요

Databricks Job을 구성해 정형 데이터 처리 파이프라인을 실행합니다.

---

## 2. 처리 레이어

### Bronze

- 경로: `landing/data/<dataset>/`
- 새로운 Parquet 파일 도착 시 Incremental Ingest 수행
- Delta Table로 적재
- 원본 데이터 보존
- Ingestion Metadata 추가 (예: `_ingestion_ts`, `_bronze_source`)

---

### Silver

- 기본 데이터 정제 수행
  - 날짜 및 숫자 타입 캐스팅
  - Null Key 제거
  - 중복 데이터 제거
  - 컬럼명 표준화

---

### Gold

- Agent 조회용 고객 포트폴리오 요약 Materialized View 생성
- 포함 데이터:
  - 고객 기본 정보
  - 현금 보유 정보
  - ETF 보유 정보
  - ETF 마스터 정보
  - 최근 가격 및 거래 요약

- 활용 예시:
  - 고객의 등급 및 리스크 프로파일
  - 현금 및 ETF 보유 기반 총자산 추정
  - 포트폴리오 구성 요약

- Materialized View 기반으로 빠른 조회 성능 제공

---

## 3. 실행 방법

0. **카탈로그에서 자기 자신에 대해 Grant Privileges**

1. utilities.common_utils 변경
    - CATALOG 이름 변경
    - LANDING_BASE 스토리지 이름 변경

2. Databricks Pipeline 실행
   - Task에서 `main_pipeline` Notebook 선택
   - Compute에서 생성한 All Purpose Compute 선택

3. 파라미터 입력
   - `usernumber`
   - `adls_access_key`
   - `storage_account`

4. Pipeline 실행
   - `Run Now` 클릭

5. 결과 확인
   - Unity Catalog에서 Bronze / Silver / Gold 테이블 및 뷰 확인

---

## 4. 자동 실행 테스트

1. File Arrival Trigger 활성화

2. 파라미터 조정
   - `abfss://landing@adlsworkshopadb.dfs.core.windows.net/data/customer_cash_portfolio/` 경로 지정
   - 10분 간격, 1분 후 실행 지정
   - 필요에 따라 경로의 스토리지 이름 업데이트

3. 데이터 추가 생성
   - `create_data`에서 일주일치 데이터 추가 생성

4. 동작 확인
   - 파일이 ADLS에 도착하면 Pipeline 자동 실행
   - 각 레이어(Bronze → Silver → Gold) 업데이트 여부 확인