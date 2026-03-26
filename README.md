# Azure Databricks ETF Workshop

## 개요 (Overview)

본 워크샵은 Azure Databricks를 활용하여 **정형 데이터 + 비정형 데이터를 통합한 End-to-End 데이터 및 AI 파이프라인**을 구축하는 것을 목표로 합니다.

ETF 데이터를 기반으로 다음을 구현합니다.

- Lakehouse 기반 데이터 플랫폼 구축
- Medallion Architecture (Bronze → Silver → Gold)
- BI 및 Text-to-SQL (Genie)
- Vector Search + LLM 기반 질의응답

---

## 워크샵 아젠다 (총 5시간, Q&A 포함)

| 주제 | 시간 | 내용 |
|------|------|------|
| 1. Databricks 개요 | 30분 | Lakehouse, Delta Lake, Unity Catalog |
| 2. 정형 데이터 파이프라인 | 60분 | Lakeflow Pipeline 기반 Medallion Architecture |
| 점심 | 60분 | 11:40 - 12:40 |
| 3. SQL 워크로드 | 30분 | BI 대시보드 + Genie (Text-to-SQL) |
| 4. AI 워크로드 | 60분 | 문서 파싱, 임베딩, Vector Search |
| 5. Microsoft Foundry | 15분 | Genie 연계 (설명 중심) |

---

## Quick Start

아래 순서대로 진행해 주시면 됩니다.

---

### 1. Databricks에 GitHub 레포 연동 (Read-only)

- 본 레포를 Databricks Workspace에 연동합니다.

- https://github.com/suzyvaque/AzureDatabricks-ETF-Workshop 링크 복사

---

### 2. 정형 데이터 파이프라인 실행

- Lakeflow Pipeline 기반 Medallion Architecture 구성
- Bronze → Silver → Gold 데이터 처리

[정형 데이터 파이프라인 가이드](./CreateStructuredPipeline.md)

---

### 3. SQL 워크로드 (BI + Genie)

- Gold 데이터 Query
- BI Dashboard 생성
- Genie Text-to-SQL 활용

[SQL 워크로드 가이드](./SQLWorkload.md)

---

### 4. 비정형 데이터 처리 (RAG)

- PDF 파싱 및 Chunking
- Embedding 생성
- Vector Search 및 LLM 질의응답

[RAG 파이프라인 가이드](./CreateRAG.md)

---