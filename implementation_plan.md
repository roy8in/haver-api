# Implementation Plan

## 목적
- 현재 코드는 Haver 수집, DB 업서트, 후처리 계산까지 한 번에 수행합니다.
- 수정은 하지 않고, 디버깅이 필요한 지점과 개선 우선순위를 정리합니다.

## 핵심 진단 요약

### 1. 가장 먼저 확인할 버그 후보
1. `main.py:76-78`
   `pk in db_max_dates and old_mod == new_mod` 조건만으로 바로 skip 합니다. 이 분기 때문에 DB에 데이터가 일부만 있어도 `datetimemod`가 같으면 실제 시계열 수집이 생략될 수 있습니다.
2. `db_handler.py:177-183`
   `update_set`이 빈 문자열이 될 수 있습니다. `date`만 있는 테이블이 생기거나, non-key 컬럼이 없는 경우 `DO UPDATE SET` 뒤가 비어 SQL 오류가 날 수 있습니다.
3. `haver_provider.py:88`
   `data.columns = ticker_names`는 Haver 응답 컬럼 수와 요청 ticker 수가 다르면 바로 예외가 납니다. 일부 ticker 실패, 중복 ticker, 응답 축소가 있으면 깨질 가능성이 큽니다.
4. `data_processor.py:50`
   `pivot()`는 같은 `date`와 `ticker_pk` 조합이 중복되면 `ValueError`를 냅니다. 업서트 전후 중복, 리비전 중복, API 응답 중복이 있으면 후처리 전체가 중단될 수 있습니다.
5. `db_handler.py:38`
   `verify=False`로 SSL 검증을 끄고 있어서 인증서 문제를 숨길 수 있습니다. 연결 실패 원인 파악이 어려워지고 운영 리스크도 큽니다.

### 2. 디버깅 중점 구간
1. `main.py`
   메타데이터 비교, skip 조건, fetch start 계산, 청크 단위 수집.
2. `haver_provider.py`
   metadata 응답 형태, series 응답 컬럼 수, chunk 실패 시 개별 fallback 결과.
3. `db_handler.py`
   SQL 생성 문자열, API 응답 파싱, 실패 로그 품질.
4. `data_processor.py`
   raw 조회 조건, pivot/resample 가정, suffix 매칭 정확도.

## 상세 개선 계획

### Phase 1. 수집 스킵 로직 검증
- 대상: `main.py:69-99`
- 확인 내용:
  - `datetimemod`가 같아도 DB의 max date가 메타데이터 end date보다 과거인 케이스가 없는지 확인
  - 신규 ticker, 부분 적재 ticker, revision ticker가 정확히 분기되는지 확인
  - `skipped_unchanged`와 `skipped_up_to_date`가 의미상 겹치지 않는지 확인
- 권장 디버깅 로그:
  - `ticker_pk`, `db_last`, `m_start`, `m_end`, `old_mod`, `new_mod`, 최종 결정(`skip`/`fetch`)
- 개선 방향:
  - skip 조건을 “수정시각 동일” 하나로 결정하지 말고 “DB 최신일 + 수정시각 동일”을 함께 확인
  - skip 사유를 명시적으로 분리

### Phase 2. Haver 응답 형태 방어 강화
- 대상: `haver_provider.py:14-58`, `haver_provider.py:60-96`
- 확인 내용:
  - `Haver.metadata()`가 DataFrame 외에 dict, list, None을 반환하는 경우
  - `database`, `code` 컬럼이 누락된 metadata 응답
  - `Haver.data()` 결과 컬럼 수와 `ticker_chunk` 길이가 다른 경우
  - chunk 실패 후 개별 재시도에서 어떤 ticker가 탈락하는지 추적 가능한지
- 권장 디버깅 로그:
  - 요청 ticker 개수, 응답 shape, 개별 fallback 성공/실패 ticker 목록
- 개선 방향:
  - 응답 schema 검증 추가
  - 컬럼 수 mismatch 시 원인과 ticker 목록을 남기고 안전 종료
  - 예외를 삼키지 말고 ticker별 실패 사유를 최소한 로그로 보존

### Phase 3. DB 계층 SQL 안정성 점검
- 대상: `db_handler.py:31-43`, `db_handler.py:107-187`
- 확인 내용:
  - API 에러 본문이 `raise_for_status()` 뒤에 가려지는지
  - `create_table_with_types()`가 기존 테이블 스키마 변경을 전혀 반영하지 못하는지
  - 문자열 기반 INSERT SQL이 큰 payload에서 깨지지 않는지
  - 숫자/불리언/날짜를 전부 문자열로 감싸는 방식이 API/DB에서 안전한지
- 권장 디버깅 로그:
  - 실패한 SQL의 table name, chunk size, 응답 status code, response body 일부
- 개선 방향:
  - `_extract_rows()`를 모든 조회 함수에서 일관되게 사용
  - bare `except` 제거 후 예외 유형별 처리
  - `update_set`이 비는 경우 `DO NOTHING` 분기 추가 검토
  - `verify=False` 제거 또는 설정값 기반 토글

### Phase 4. 후처리 파이프라인 정확성 검증
- 대상: `data_processor.py:9-56`, `processors/policy_rate.py`, `processors/pmi.py`
- 확인 내용:
  - `ILIKE '%%suffix%%'`가 의도보다 넓게 매칭되는지
  - `rtar`/`rtat` 변수명 혼용 때문에 잘못된 suffix를 사용하는지
  - 중복 데이터가 있을 때 `pivot()`에서 예외가 나는지
  - 월말 리샘플링이 일간/주간/월간 원본에 모두 타당한지
  - `filter_recent_data()`가 실제로 호출되지 않는 dead code인지
- 권장 디버깅 로그:
  - suffix별 원본 row 수, 중복 `(date, ticker_pk)` 건수, resample 전후 기간 범위
- 개선 방향:
  - 정확한 ticker 규칙으로 조회 조건 축소
  - 후처리 전에 중복 검증 단계 추가
  - 사용하지 않는 함수와 import 정리

## 테스트 보강 계획

### 우선 추가할 테스트
1. `main.py` skip 로직
   - `datetimemod` 동일하지만 `db_last < m_end`인 경우 fetch 되는지
2. `haver_provider.py` 응답 형태
   - metadata dict 반환
   - series 응답 컬럼 수 mismatch
   - chunk 실패 후 일부 ticker만 개별 복구
3. `db_handler.py` SQL 생성
   - non-key 컬럼 없는 upsert
   - 문자열에 quote 포함
   - null, float, bool 혼합 row
4. `data_processor.py`
   - 중복 `(date, ticker_pk)` 입력 시 처리 방식
   - 빈 결과, 단일 ticker, 월별 누락 데이터

### 테스트 방식
- 외부 API 의존성이 커서 단위 테스트에서는 `Haver`와 `db.send_sql`을 mock 처리
- 실제 장애 추적용으로는 소량 ticker 샘플에 대한 통합 테스트 스크립트 분리 권장

## 실행 순서 제안
1. `main.py`의 skip 조건부터 재현 로그 추가 후 동작 확인
2. `haver_provider.py`의 응답 shape 검증 추가 방향 설계
3. `db_handler.py`의 SQL 실패 로그와 SSL 설정 정리
4. `data_processor.py`의 중복/매칭 규칙 검증
5. 마지막에 테스트 케이스 추가

## 참고 메모
- 현재 워크트리에 사용자 변경 사항이 이미 있으므로, 이후 실제 수정 시에는 기존 변경과 충돌 여부를 먼저 확인해야 합니다.
- 콘솔 출력 한글이 깨져 보이는 구간이 있어 인코딩 또는 터미널 설정 점검도 함께 권장합니다. 로그 가독성이 떨어지면 디버깅 시간이 크게 늘어납니다.
