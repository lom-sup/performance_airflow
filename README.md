# performance_airflow
airflow 등을 이용한 한국 공연문화 분석

<br/>

# 공연 장르별 통계 데이터 파이프라인 구축

**개요**
이 코드에서는 KOPIS(Open API)를 활용하여 공연 장르별 통계를 수집하고, 이를 S3와 Redshift에 적재하는 Airflow 기반 ETL 파이프라인을 구현하였습니다. 또한, 수집된 데이터를 기반으로 2024년 공연 시장 트렌드 분석을 위한 대시보드를 구축할 계획입니다.

**구현 내용**
1. 데이터 수집 (Extract)
KOPIS API를 호출하여 전년도(2024) 공연 장르별 월간 통계 데이터를 수집
Airflow에서 _task_get_genre_data_ 태스크로 실행

2. 데이터 저장 및 전송 (Load)
변환된 JSON 데이터를 AWS S3에 저장
Airflow의 _task_genre_data_to_s3_ 태스크에서 S3 업로드 처리

3. 데이터 적재 및 변환 (Transform)
S3에서 JSON 데이터를 가져와 AWS Redshift의 KOPIS_{연도}_genre 테이블에 적재
Airflow의 _task_s3_to_redshift_ 태스크에서 실행
테이블이 존재하지 않을 경우 자동 생성하며, 레코드 단위 삽입

**Task 실행순서**
task_get_genre_data >> task_genre_data_to_s3 >> task_s3_to_redshift

## 함수 설명

<details>
  <summary> xml_to_json(xml_files, json_files)</summary>

   **XML 데이터를 JSON으로 변환 후 저장하는 함수**  
  - XML 파일을 읽어 JSON 형식으로 변환  
  - 변환된 JSON을 파일로 저장  
  - 파일이 비어있으면 오류 로그를 남기고 변환하지 않음  

</details>

<details>
  <summary> get_genre_data(**context)</summary>

   **KOPIS API에서 공연 장르별 데이터를 가져와 XML 및 JSON 파일로 저장하는 함수**  
  - 실행 연도의 전년도 데이터를 가져옴 (`execution_date.year - 1`)  
  - KOPIS API를 월별로 호출하여 데이터를 수집  
  - API 응답을 XML로 변환 후 JSON으로 변환  
  - 데이터를 `/opt/airflow/data/kopis_{year}_genre_stats.xml` 및 `.json` 파일로 저장  

</details>

<details>
  <summary> genre_data_to_s3(**context)</summary>

   **변환된 JSON 데이터를 AWS S3에 업로드하는 함수**  
  - 전년도(`execution_date.year - 1`) JSON 파일을 지정된 S3 버킷에 업로드  
  - S3 키 경로: `"kopis_genre_data/kopis_{year}_genre_stats.json"`  
  - 업로드 성공 여부를 로깅  

</details>

<details>
  <summary> s3_to_redshift(**context)</summary>

   **S3에 저장된 JSON 데이터를 가져와 AWS Redshift에 적재하는 함수**  
  - S3에서 JSON 데이터를 읽고 파싱  
  - 데이터를 월별(`year_month`)로 변환하여 Redshift 테이블에 저장  
  - 테이블(`KOPIS_{year}_genre`)이 없으면 자동 생성  
  - `INSERT`를 사용해 레코드 단위로 적재하며, 100건마다 커밋 수행  

</details>

<details>
  <summary> DAG 정의 (genre_s3_redshift)</summary>

   **Airflow DAG을 정의하여 ETL 파이프라인을 실행**  
  - **`task_get_genre_data`**: KOPIS API에서 데이터를 수집하고 JSON 변환  
  - **`task_genre_data_to_s3`**: 변환된 JSON 데이터를 S3에 업로드  
  - **`task_s3_to_redshift`**: S3에서 데이터를 가져와 Redshift에 적재  
  - `@once` (한 번 실행)으로 설정되어 있지만, 실제 운영에서는 `@yearly`로 변경 가능  

</details>

## xml 예시
```
<?xml version='1.0' encoding='utf-8'?>
<kopis_genre_stats><month year-month="202401"><prfst>
        <cate>연극</cate>
        <amount>5815285915</amount>
        <nmrs>221722</nmrs>
        <prfdtcnt>3918</prfdtcnt>
        <nmrsshr>13.7</nmrsshr>
        <prfprocnt>123</prfprocnt>
        <amountshr>5.4</amountshr>
    </prfst>
    <prfst>
        <cate>뮤지컬</cate>
        <amount>46121809978</amount>
        <nmrs>731362</nmrs>
        <prfdtcnt>3703</prfdtcnt>
        <nmrsshr>45.3</nmrsshr>
        <prfprocnt>177</prfprocnt>
        <amountshr>43.2</amountshr>
    </prfst>
    <prfst>
        <cate>서양음악(클래식)</cate>
        <amount>5852228480</amount>
        <nmrs>164439</nmrs>
        <prfdtcnt>426</prfdtcnt>
        <nmrsshr>10.2</nmrsshr>
        <prfprocnt>363</prfprocnt>
        <amountshr>5.5</amountshr>
    </prfst>
```


## Table

필드명 | 설명 | 샘플데이터
-- | -- | --
year_month | 년-월 | 202401
cate | 장르 | 연극
amount | 티켓판매액 | 1496352101
nmrs | 티켓판매수 | 55345
prfdtcnt | 상연횟수 | 1003
nmrsshr | 관객점유율 | 12.8
prfprocnt | 개막편수 | 41
amountshr | 티켓판매액 점유율 | 4.9


## Table 예시
<img width="2102" height="820" alt="Image" src="https://github.com/user-attachments/assets/81425017-7532-40cb-bc5d-f90ac831e75d" />
