# SQL 워크로드 테스트

## 1. SQL Query


## 2. SQL Dashboard

### 테이블 (분석 관점)

Gold Layer 기반 주요 KPI 시각화

* 고객별 자산 분포
* ETF 보유 비중
* 리스크 등급별 투자 현황

### 메타데이터 관리 (운영 관점)

System Table 기반 파이프라인 실행 메타데이터 시각화

* 파이프라인 실행 시간
* 처리 레코드 수
* 테이블별 업데이트 시점
* 실패 작업 로그

## 3. Genie

Genie에 다음 테이블을 연결합니다.

* Gold Materialized View
* Silver Master 테이블
* Vector Search Index

### 예시 질문:

* 투자자의 나이와 자산 규모에 따른 투자 경향을 분석해줘.
* 안정형 일반 투자자에 맞는 종목을 추천해줘.

### Chat Mode vs Agent Mode

| 구분     | Chat     | Agent           |
| ------ | -------- | --------------- |
| 동작 방식  | 단순 질의응답  | 목표 기반 작업 수행     |
| 데이터 활용 | 단일 질의 중심 | 여러 단계 reasoning |
| 활용도    | 빠른 조회    | 분석 및 추천         |
