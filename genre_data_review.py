import requests
import calendar
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import xmltodict
import json
import logging
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 전역 변수
SERVICE_KEY = Variable.get("KOPIS_SERVICE_KEY", default_var="b334b3a40adc4d40bc3211b94b1c7d11")
S3_bucket = "performance-airflow-e2e"
S3_folder = "kopis_genre_data"
AWS_CONN_ID = "myaws"


def xml_to_json(xml_files, json_files):
    """XML을 JSON으로 변환 후 저장"""
    
    # 파일 비어있는지 확인
    if not os.path.exists(xml_files) or os.stat(xml_files).st_size == 0:
        logging.error(f"XML 파일이 비어 있음: {xml_files}. JSON 변환 생략")
        return
    
    with open(xml_files, 'r', encoding='utf-8') as f:
        xml_data = f.read()
    
    # xml을 dict 형태로 파싱
    json_data = xmltodict.parse(xml_data)
    
    # Json으로 저장
    with open(json_files, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=4)
        

def get_genre_data(**context):
    """API에서 장르별 공연 데이터를 가져와 XML 저장 후 JSON 변환"""
    base_url = "http://www.kopis.or.kr/openApi/restful/prfstsCate"
    year = context["execution_date"].year - 1 # 작년 연도 구하기
    
    # 새 요소 생성
    root = ET.Element("kopis_genre_stats")  

    # 월마다 장르 정보 가져오기
    for month in range(1, 13):
        
        # 각 월 1일과 말일 구하기
        startday = datetime(year, month, 1).strftime("%Y%m%d")
        endday = datetime(year, month, calendar.monthrange(year, month)[1]).strftime("%Y%m%d")

        # xml 요소에 속성 추가
        month_data = ET.Element("month", attrib={"year-month": f"{year}{month:02d}"})
        params = {"service": SERVICE_KEY, "stdate": startday, "eddate": endday} # API 파라미터 지정

        # 응답 받아오기고 상태 코드 확인
        response = requests.get(base_url, params=params, timeout=30)
        
        if response.status_code != 200 or not response.content.strip():
            logging.error(f"API 호출 실패 또는 빈 응답: {base_url}, 상태 코드: {response.status_code}")
            return  # 빈 파일을 생성하지 않고 함수 종료
        
        responses = ET.fromstring(response.content) # xml 문자열을 트리로 변환

        prfst_elements = responses.findall("prfst") # prfst 요소를 리스트로
        for prfst in prfst_elements:
            month_data.append(prfst) # month 안에 리스트 요소 추가하기
            
        root.append(month_data) # month를 가장 처음 생성한 요소 아래 추가 
        
        if len(root) == 0:
            logging.warning(f"{year}년 전체 데이터 없음. XML 저장하지 않음.")
            return

    xml_files = f"/opt/airflow/data/kopis_{year}_genre_stats.xml"
    tree = ET.ElementTree(root)
    tree.write(xml_files, encoding='utf-8', xml_declaration=True)

    json_file = f"/opt/airflow/data/kopis_{year}_genre_stats.json"
    xml_to_json(xml_files, json_file)

    logging.info(f"{year} 장르 데이터 저장 완료")
    

def genre_data_to_s3(**context):
    """저장된 JSON 파일을 S3에 업로드"""
    
    year = context["execution_date"].year - 1
    
    # 저장 json 파일 경로(컨테이너 안)
    json_file = f"/opt/airflow/data/kopis_{year}_genre_stats.json"
    
    # 저장할 S3 위치
    s3_key = f"{S3_folder}/kopis_{year}_genre_stats.json"

    # S3 업로드
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_hook.load_file(filename=json_file, key=s3_key, bucket_name=S3_bucket, replace=True)

    logging.info(f"S3 업로드 완료: s3://{S3_bucket}/{s3_key}")
    

def s3_to_redshift(**context):
    """S3에서 JSON 데이터를 가져와 Redshift에 적재"""
    year = context["execution_date"].year - 1
    redshift_hook = PostgresHook(postgres_conn_id="myredshift") # Redshift 연결
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    
    # 실수, 로컬에서 파일 가져옴
    '''
    json_file = f"/opt/airflow/data/kopis_{year}_genre_stats.json" # Json 파일 경로

    # 파일 열고 data에 넣기
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # record(행)별로 생성
    records = []
    for month_data in data["kopis_genre_stats"]["month"]:
        year_month = month_data["@year-month"]
        
        for prfst in month_data["prfst"]:
            each_record = (
                year_month,
                prfst["cate"],
                int(prfst["amount"]),
                int(prfst["nmrs"]),
                int(prfst["prfdtcnt"]),
                float(prfst["nmrsshr"]),
                int(prfst["prfprocnt"]),
                float(prfst["amountshr"])
            )
            records.append(each_record)
    '''
    
    # S3 파일 경로 및 키 (S3에 저장된 JSON 파일)
    s3_key = f"{S3_folder}/kopis_{year}_genre_stats.json"
    # S3 객체를 문자열로 읽어오기
    json_str = s3_hook.read_key(key=s3_key, bucket_name=S3_bucket)
    
    #JSON 파싱
    data = json.loads(json_str)
    
    # 각 월별, 각 prfst 레코드를 순회하며 레코드 생성
    records = []
    months = data.get("kopis_genre_stats", {}).get("month", [])
    # 만약 month가 단일 dict라면 리스트로 변환
    if isinstance(months, dict):
        months = [months]
    
    for month_data in months:
        # XML 파일에서 속성은 "@year-month"로 되어있음. 없으면 "year_month"로 시도
        year_month = month_data.get("@year-month") or month_data.get("year_month")
        prfst_entries = month_data.get("prfst", [])
        # prfst_entries가 dict인 경우 단일 객체이므로 리스트로 변환
        if isinstance(prfst_entries, dict):
            prfst_entries = [prfst_entries]
        for prfst in prfst_entries:
            try:
                record = (
                    year_month,
                    prfst["cate"],
                    int(prfst["amount"]),
                    int(prfst["nmrs"]),
                    int(prfst["prfdtcnt"]),
                    float(prfst["nmrsshr"]),
                    int(prfst["prfprocnt"]),
                    float(prfst["amountshr"])
                )
                records.append(record)
            except Exception as e:
                logging.error(f"레코드 파싱 오류: {e} / 데이터: {prfst}")

    # 테이블 없으면 생성
    create_genre_table_sql = f"""
        CREATE TABLE IF NOT EXISTS KOPIS_{year}_genre (
            year_month VARCHAR(6) NOT NULL,
            cate VARCHAR(50) NOT NULL,
            amount BIGINT,
            nmrs INT,
            prfdtcnt INT,
            nmrsshr NUMERIC(5,2),
            prfprocnt INT,
            amountshr NUMERIC(5,2)
        );
    """
    redshift_hook.run(create_genre_table_sql)
    logging.info(f"KOPIS_{year}_genre 테이블 생성 완료")

    # 테이블 레코드 삽입
    redshift_hook.insert_rows(
        table=f"KOPIS_{year}_genre",
        rows=records,
        target_fields=["year_month", "cate", "amount", "nmrs", "prfdtcnt", "nmrsshr", "prfprocnt", "amountshr"],
        commit_every=100
    )
    logging.info(f"[KOPIS_{year}_genre2] {len(records)} 건의 데이터가 Redshift에 적재 완료")


# 대그 만들기
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'genre_s3_redshift',
    default_args=default_args,
    schedule_interval="@once", #"@yearly" #1년마다 실행
    catchup=False,
    description="Kopis 공연통계-장르별 통계 파이프라인(S3-Redshift 적재)"
) as dag:
    
    task_get_genre_data = PythonOperator(
        task_id = 'task_get_genre_data',
        python_callable= get_genre_data
    )

    task_genre_data_to_s3 = PythonOperator(
        task_id = 'task_genre_data_to_s3',
        python_callable = genre_data_to_s3 
    )
    
    task_s3_to_redshift = PythonOperator(
        task_id = 'task_s3_to_redshift',
        python_callable= s3_to_redshift
    )
    
    task_get_genre_data >> task_genre_data_to_s3 >> task_s3_to_redshift # 데이터 추출 - S3 - Redshift적재 차례로 실행 