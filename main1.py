import ee
import os
import json
import sys

# 1. 시크릿 키 가져오기
key_content = os.getenv('GEE_SERVICE_ACCOUNT_KEY')

# 2. [진단] 키가 제대로 들어왔는지 확인 (내용은 보안상 출력 안 함)
if not key_content:
    print("❌ [치명적 오류] GitHub Secrets에서 'GEE_SERVICE_ACCOUNT_KEY'를 찾을 수 없습니다.")
    print("👉 원인: Secrets 이름이 틀렸거나, 값이 비어있습니다.")
    sys.exit(1) # 여기서 강제로 종료! (수동 로그인 시도 자체를 차단)

print(f"✅ 시크릿 키 감지됨 (길이: {len(key_content)} 자)")

# 3. [진단] JSON 변환 시도
try:
    service_account_info = json.loads(key_content)
except json.JSONDecodeError as e:
    print(f"❌ [치명적 오류] 키 내용이 올바른 JSON 형식이 아닙니다.")
    print(f"👉 에러 내용: {e}")
    sys.exit(1)

# 4. GEE 인증 (서비스 계정 전용)
try:
    # 여기가 핵심입니다. credentials를 반드시 넣어서 초기화합니다.
    credentials = ee.ServiceAccountCredentials(
        email=service_account_info['client_email'], 
        key_data=key_content # key_data에 직접 문자열을 넣습니다.
    )
    
    # 인증 정보를 강제로 주입합니다. (프로젝트 ID 필수)
    ee.Initialize(credentials, project='absolute-cache-478407-p5')
    
    print("🎉 [성공] GEE 서비스 계정 인증 완료! (이제 자동화가 가능합니다)")

except Exception as e:
    print(f"❌ [GEE 초기화 실패] : {e}")
    sys.exit(1)

# --- 인증이 성공해야만 아래 코드가 실행됨 ---
print("🛰️ (테스트) 위성 데이터 접근 가능 여부 확인 중...")
try:
    # 간단한 테스트: SRTM 지형 데이터 정보 가져오기
    img = ee.Image('USGS/SRTMGL1_003')
    info = img.getInfo()
    print("✅ 데이터 접근 성공! GEE가 정상 작동합니다.")
except Exception as e:
    print(f"❌ 데이터 접근 실패: {e}")