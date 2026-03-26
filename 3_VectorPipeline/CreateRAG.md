# 비정형 데이터 처리

## 1. Schema 및 External Location 설정

sch_user{usernumber}_unstructured 스키마를 생성합니다.

External Location Path를 unstructured로 지정합니다.

## 2. Parsing 및 Chunking

[Parsing and Chunking Notebook](./Parsing_and_Chunking.ipynb)을 실행합니다.

* PDF → 텍스트 파싱
* Chunk 단위로 분할

### 주의사항

다음 단계에서 임베딩 코드를 작성하는 대신, Databricks Vector Search UI를 활용해 델타 싱크 방식으로 임베딩 및 인덱싱합니다.

* [공식 문서 참고](https://docs.databricks.com/aws/en/vector-search/create-vector-search)

## 3. Change Data Feed 설정

Vector Search 인덱싱을 위해 Chunk 테이블에 CDF 활성화가 필요합니다.

[Vector Search Notebook](./Vector_Search.ipynb)에서 다음 설정을 적용합니다.

```
ALTER TABLE <chunk_table_name>
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

## 4. Vector Search UI 구성

### 4.1 Compute 생성

Compute에서 Vector Search Endpoint를 생성합니다.

### 4.2 Index 생성

Create Index 선택 후 Chunk 테이블을 지정합니다.

* Primary Key: chunk_id
* Embedding Source Column: chunk_text
* Embedding Model: databricks-gte-large-en
* Sync Compute Embeddings

## 5. Playground 테스트

Index 생성 완료 후 Try in Playground에서 RAG를 테스트할 수 있습니다. (LLM 기반 Retrieval + Generation 자동 수행)

### 예시 질문

* 레버리지 투자에 대해 알려줘
* 레버리지보다 안정적인 ETF를 추천해줘
* 빅테크 관련 ETF와 주요 리스크를 정리해줘
