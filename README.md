# Haver API Data Sync Project

이 프로젝트는 Haver Analytics API를 통해 경제 데이터를 수집하고, 이를 PostgreSQL DB API를 통해 원격 데이터베이스에 업로드하는 자동화 도구입니다.

## 주요 기능
- **스마트 필터링**: DB에 저장된 마지막 날짜를 자동으로 확인하여 신규 데이터만 증분 수집합니다.
- **가공 데이터 생성 (Diffusion Index)**: 수집된 원시 데이터를 바탕으로 기준금리 및 PMI 확산지수(DI)를 자동 산출합니다.
  - **기준금리 (Policy Rate)**: 3개월 전 대비 금리 변화 확산지수 및 국가별 변화폭 저장.
  - **PMI (Mfg/Srv)**: 50 초과 여부 기준 확산지수 및 3개월 이동평균(3MA) 산출.
- **데이터 보정 (Forward-fill)**: 국가별 발표 시점 차이를 고려하여 최대 1개월까지 이전 데이터를 유지하여 지수 왜곡을 방지합니다.
- **효율적 요청 (Batch Processing)**: 티커들을 주기별, 날짜별로 그룹화하여 API 호출 횟수를 최소화합니다.
- **자동 테이블 생성**: 데이터 구조에 따라 DB 테이블을 자동으로 생성하고 Upsert를 수행합니다.

## 프로젝트 구조
- `main.py`: 전체 수집 및 가공 프로세스를 제어하는 메인 실행 파일.
- `data_processor.py`: 지표별 프로세서를 호출하여 가공 데이터를 생성하는 오케스트레이터.
- `processors/`: 개별 지표 처리 로직 모듈 폴더.
  - `policy_rate.py`: 기준금리 변화분 및 DI 계산.
  - `pmi.py`: 제조업/서비스업 PMI DI 및 3MA 계산.
- `haver_provider.py`: Haver API 통신 및 데이터 전처리 전담.
- `db_handler.py`: 데이터베이스 API 통신 및 SQL 실행 전담.
- `tickers.csv`: 수집 대상 티커 목록 설정 파일.
- `.env`: API 키 및 URL 설정 파일.

## 설치 및 설정 방법

### 1. 가상환경 설정 및 라이브러리 설치
```powershell
# 가상환경 생성
python -m venv .venv

# 가상환경 활성화 (Windows)
.\.venv\Scripts\activate

# 필수 라이브러리 설치
pip install -r requirements.txt

# Haver 라이브러리 설치 (별도 권한 필요)
pip install Haver --extra-index-url http://www.haver.com/Python --trusted-host www.haver.com
```

### 2. 환경 변수 설정
`.env` 파일을 생성하고 다음 정보를 입력합니다:
```env
POSTGRE_API_URL=your_api_url
POSTGRE_API_KEY=your_api_key
CERT_PATH_ENV=your_cert_path (필요시)
POSTGRE_VERIFY_SSL=true
HAVER_INIT_TIMEOUT_SECONDS=30
```

- `POSTGRE_VERIFY_SSL=false`로 두면 인증서 검증 없이 DB API를 호출합니다. 현재처럼 사내/중간 인증서 문제로 SSL 검증이 실패할 때의 임시 우회용입니다.
- `CERT_PATH_ENV`에 CA 번들 경로를 넣을 수 있으면 그쪽이 더 안전합니다.

### 3. 티커 리스트 작성
`tickers.csv` 파일에 수집할 티커를 입력합니다 (예: `usecon:gdp`).

## 실행 방법
```powershell
python main.py
```

## 참고 사항
- 본 프로그램은 Haver Analytics 웹 구독 버전(Direct 1)에 최적화되어 있습니다.
- Revision 데이터 반영을 위해 매 수집 시 마지막 저장일로부터 180일 전부터의 데이터를 다시 확인합니다.
