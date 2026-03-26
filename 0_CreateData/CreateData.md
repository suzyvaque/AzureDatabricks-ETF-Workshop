# 배경

Databricks Workspace에 연결된 스토리지 계정에 데이터가 이미 랜딩되어 있다고 가정합니다.

데이터는 파케이(Parquet) 파일 형태로 저장되며, 파일 트리거 방식으로 후속 전처리 파이프라인을 실행하고자 합니다.

# 실행 방법

## 1. Job 생성

Databricks에서 Job을 생성합니다.

* Task 유형: Notebook 선택
* Notebook 경로: create_data.py 지정

## 2. Compute 설정

**All Purpose Compute**를 생성해 연결합니다.

(Single node: Standard_D4ds_v5, Terminate after 300 minutes of inactivity)

* install_requirements 노트북에 해당 Compute를 연결해 노트북을 실행합니다.
* 필요한 라이브러리를 미리 설치해, 서버리스 환경에서 매번 faker 등을 로드하지 않도록 하기 위함입니다.

## 3. 파라미터 설정

다음 파라미터를 설정합니다.

* default_start_date: 2026_01_01
* default_end_date: 2026_02_28
* krx_auth_key: 워크스페이스의 txt 파일 참고
* adls_access_key: 스토리지에서 확인

## 4. Job 실행

Task를 생성한 후 Run Now를 통해 Job을 실행합니다.

실행 시 Landing 컨테이너의 Data 폴더 내에 데이터가 생성됩니다.

### 추가 데이터 생성 방법

추가 데이터를 생성할 경우, Job Task의 파라미터를 다음과 같이 수정합니다.

* default_start_date: 기존 종료일 기준으로 +1
* default_end_date: 원하는 종료 날짜로 설정

이후 동일하게 Job을 실행하면 해당 기간에 대한 데이터가 추가로 생성됩니다.
