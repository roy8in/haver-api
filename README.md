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
- `app/`: 실행 로직과 공용 유틸리티 패키지.
  - `data_processor.py`: 지표별 프로세서를 호출하여 가공 데이터를 생성하는 오케스트레이터.
  - `processors/`: 개별 지표 처리 로직 모듈.
    - `policy_rate.py`: 기준금리 변화분 및 DI 계산.
    - `pmi.py`: 제조업/서비스업 PMI DI 및 3MA 계산.
  - `haver_provider.py`: Haver API 통신 및 데이터 전처리 전담.
  - `db_handler.py`: 데이터베이스 API 통신 및 SQL 실행 전담.
  - `dashboard_state.py`: 대시보드용 상태 JSON 생성 및 게시.
  - `alerts.py`: 팝업 및 이메일 알림 전송.
  - `run_logging.py`: 실행 로그와 요약 CSV 기록.
- `tickers.csv`: 수집 대상 티커 목록 설정 파일.
- `.env`: API 키 및 URL 설정 파일.
- `state/`: GitHub로 게시되는 최신 상태 및 실패 기록.
- `docs/`: 상태 JSON 계약과 대시보드 데이터 계약 문서.

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
HAVER_INIT_MAX_ATTEMPTS=2
HAVER_INIT_RETRY_DELAY_SECONDS=5
HAVER_REQUIRE_AUTH_READY=false
HAVER_ALERT_POPUP=true
HAVER_ALERT_SMTP_HOST=
HAVER_ALERT_SMTP_PORT=587
HAVER_ALERT_SMTP_USERNAME=
HAVER_ALERT_SMTP_PASSWORD=
HAVER_ALERT_FROM=
HAVER_ALERT_TO=
HAVER_ALERT_SMTP_STARTTLS=true
HAVER_NODE_ROLE=company
HAVER_GITHUB_PUBLISH_ENABLED=false
HAVER_GITHUB_COMMIT_MESSAGE=Update dashboard state
HAVER_GITHUB_PUSH_REMOTE=origin
HAVER_GITHUB_PUSH_BRANCH=
```

- `POSTGRE_VERIFY_SSL=false`로 두면 인증서 검증 없이 DB API를 호출합니다. 현재처럼 사내/중간 인증서 문제로 SSL 검증이 실패할 때의 임시 우회용입니다.
- `CERT_PATH_ENV`에 CA 번들 경로를 넣을 수 있으면 그쪽이 더 안전합니다.
- `HAVER_INIT_MAX_ATTEMPTS`와 `HAVER_INIT_RETRY_DELAY_SECONDS`는 Haver 초기화가 느리거나 일시적으로 멈출 때의 재시도 동작을 조절합니다.
- `HAVER_REQUIRE_AUTH_READY=true`로 두면 예약 실행 전에 Haver 로그인 상태가 없을 때 바로 중단하고 알람을 보냅니다.
- `HAVER_ALERT_POPUP=true`는 콘솔 실행 중 즉시 팝업 알림을 띄웁니다. SMTP 값을 채우면 이메일 알림도 보낼 수 있습니다.
- `HAVER_GITHUB_PUBLISH_ENABLED=true`로 두면 실행 종료 후 `state/haver_status.json`과 `state/haver_events.jsonl`을 GitHub에 커밋/푸시합니다.
- `HAVER_NODE_ROLE`은 대시보드에서 이 실행이 회사 PC인지 집 PC인지 구분하는 라벨입니다.

### 예약 실행용 사전 점검
예약 작업에서는 먼저 아래 스크립트를 실행해 Haver 로그인 상태를 확인하는 구성이 가장 안전합니다.

```powershell
python scripts/haver_preflight.py
python main.py
```

### GitHub 게시

상태 파일만 수동으로 게시하려면 아래 스크립트를 사용할 수 있습니다.

```powershell
powershell -File scripts/publish_dashboard_state.ps1
```

### 3. 티커 리스트 작성
`tickers.csv` 파일에 수집할 티커를 입력합니다 (예: `usecon:gdp`).

## 실행 방법
```powershell
python main.py
```

## 참고 사항
- 본 프로그램은 Haver Analytics 웹 구독 버전(Direct 1)에 최적화되어 있습니다.
- Revision 데이터 반영을 위해 매 수집 시 마지막 저장일로부터 180일 전부터의 데이터를 다시 확인합니다.
