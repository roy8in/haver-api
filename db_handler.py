import os
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import urllib3

# SSL 경고 숨기기
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# .env 로드
env_path = Path(__file__).resolve().parent / '.env'
load_dotenv(env_path)

# API 설정
POSTGRE_API_URL = os.getenv("POSTGRE_API_URL")
POSTGRE_API_KEY = os.getenv("POSTGRE_API_KEY")
POSTGRE_HEADER = {
    "x-api-key": POSTGRE_API_KEY,
    "Content-Type": "application/json"
}
CERT_PATH = os.getenv("CERT_PATH_ENV")

def setup_environment():
    """인증서 설정"""
    if CERT_PATH and os.path.exists(CERT_PATH):
        os.environ['REQUESTS_CA_BUNDLE'] = CERT_PATH
    else:
        os.environ.pop('REQUESTS_CA_BUNDLE', None)

def send_sql(sql_text):
    """PostgreSQL API로 SQL 전송"""
    if not POSTGRE_API_URL or not POSTGRE_API_KEY:
        print("⚠️ API URL or Key missing in .env")
        return None
    payload = {"sql": sql_text}
    try:
        response = requests.post(POSTGRE_API_URL, json=payload, headers=POSTGRE_HEADER, verify=False)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"   ⚠️ API Request Failed: {e}")
        return None

def get_ticker_max_dates():
    """DB에 저장된 티커별 마지막 날짜 리스트 조회 (API 응답 형식에 무관하게 작동)"""
    # 1. 테이블 존재 여부 확인
    check_sql = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'haver_values')"
    res = send_sql(check_sql)
    
    try:
        table_exists = False
        if res and len(res) > 0:
            first_row = res[0]
            # [[True]] 형식인 경우
            if isinstance(first_row, list) and len(first_row) > 0:
                table_exists = first_row[0]
            # [{'exists': True}] 형식인 경우
            elif isinstance(first_row, dict):
                table_exists = list(first_row.values())[0]
        
        if not table_exists:
            return {}
    except Exception as e:
        print(f"   ℹ️ Table check failed or table doesn't exist yet: {e}")
        return {}

    # 2. 티커별 마지막 날짜 조회
    sql = "SELECT ticker_pk, MAX(date) FROM haver_values GROUP BY ticker_pk"
    result = send_sql(sql)
    
    max_dates = {}
    if result and isinstance(result, list):
        for row in result:
            try:
                pk, dt = None, None
                if isinstance(row, list) and len(row) >= 2:
                    pk, dt = row[0], row[1]
                elif isinstance(row, dict):
                    vals = list(row.values())
                    if len(vals) >= 2:
                        pk, dt = vals[0], vals[1]
                
                if pk and dt:
                    max_dates[str(pk)] = str(dt)
            except:
                continue
    return max_dates

def create_table_with_types(df, table_name):
    """데이터프레임 타입을 분석하여 테이블 생성 (PK 포함)"""
    columns_sql = []
    for col_name, dtype in df.dtypes.items():
        col_lower = col_name.lower()
        sql_type = 'TEXT'

        if col_lower == 'date':
            sql_type = 'DATE'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            sql_type = 'TIMESTAMP'
        elif pd.api.types.is_bool_dtype(dtype):
            sql_type = 'BOOLEAN'
        elif pd.api.types.is_integer_dtype(dtype):
            sql_type = 'BIGINT'
        elif pd.api.types.is_float_dtype(dtype):
            sql_type = 'DOUBLE PRECISION'
        
        if col_lower == 'ticker_pk' and table_name == 'haver_metadata':
            sql_type += " PRIMARY KEY"

        columns_sql.append(f'"{col_name}" {sql_type}')

    pk_constraint = ""
    if table_name == 'haver_values':
        pk_constraint = ', PRIMARY KEY ("ticker_pk", "date")'

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(columns_sql)}
        {pk_constraint}
    );
    """
    send_sql(create_sql)

def upsert_data(df, table_name, chunk_size=1000):
    """데이터를 DB에 Upsert"""
    if df.empty: return
    
    total_rows = len(df)
    for start in range(0, total_rows, chunk_size):
        chunk = df.iloc[start:start + chunk_size]
        values_list = []
        
        for row in chunk.itertuples(index=False, name=None):
            row_values = []
            for val in row:
                if pd.isna(val): row_values.append("NULL")
                elif isinstance(val, str):
                    safe_str = val.replace("'", "''")
                    row_values.append(f"'{safe_str}'")
                else: row_values.append(f"'{val}'")
            values_list.append(f"({', '.join(row_values)})")

        col_names = ', '.join([f'"{c}"' for c in df.columns])
        all_values = ', '.join(values_list)
        
        conflict_target = '"ticker_pk"' if table_name == 'haver_metadata' else '"ticker_pk", "date"'
        update_set = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c.lower() not in ['ticker_pk', 'date']])
        
        upsert_sql = f"""
        INSERT INTO {table_name} ({col_names}) 
        VALUES {all_values}
        ON CONFLICT ({conflict_target}) 
        DO UPDATE SET {update_set};
        """
        send_sql(upsert_sql)
