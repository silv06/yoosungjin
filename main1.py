import ee
import os
import sys
import json
from datetime import datetime, timedelta
from supabase import create_client

# --- 1. 환경 변수 점검 ---
print("🔍 환경 변수 및 인증 점검...")

gee_key_json = os.getenv('GEE_SERVICE_ACCOUNT_KEY')
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')

if not gee_key_json:
    print("❌ [오류] GEE_SERVICE_ACCOUNT_KEY가 비어있습니다.")
    sys.exit(1)

if not supabase_url or not supabase_key:
    print("❌ [오류] Supabase 설정이 비어있습니다.")
    sys.exit(1)

print(f"✅ 환경 변수 확인 완료 (GEE 키 길이: {len(gee_key_json)} 자)")

# --- 2. GEE 초기화 ---
print("🛰️ Google Earth Engine 초기화 중...")

try:
    # Windows에서 복사할 때 /n으로 저장된 경우 자동 수정
    # Linux 환경(GitHub Actions)에서 실행되므로 \n으로 변환
    gee_key_json_fixed = gee_key_json.replace('/n', '\n')
    
    # JSON 파싱
    service_account_info = json.loads(gee_key_json_fixed)
    print(f"✅ JSON 파싱 성공 (client_email: {service_account_info['client_email']})")
    
    # Private Key 추출 및 개행 문자 정규화
    private_key = service_account_info['private_key']
    
    # 혹시 private_key 내부에도 /n이 있다면 변환
    if '/n' in private_key:
        private_key = private_key.replace('/n', '\n')
        print("   ⚠️  Private Key의 /n을 \\n으로 자동 수정")
    
    # GEE 인증
    credentials = ee.ServiceAccountCredentials(
        email=service_account_info['client_email'],
        key_data=private_key
    )
    
    project_id = service_account_info.get('project_id', 'absolute-cache-478407-p5')
    
    ee.Initialize(
        credentials=credentials,
        project=project_id
    )
    
    print("✅ GEE 인증 성공!")

except json.JSONDecodeError as e:
    print(f"❌ JSON 파싱 실패: {e}")
    print(f"   위치: 문자 {e.pos}")
    print(f"   힌트: GitHub Secret에 JSON이 올바르게 저장되었는지 확인하세요.")
    sys.exit(1)
except KeyError as e:
    print(f"❌ JSON에 필수 필드 없음: {e}")
    print(f"   사용 가능한 필드: {list(service_account_info.keys())}")
    sys.exit(1)
except ee.EEException as e:
    print(f"❌ GEE 인증 실패: {e}")
    print("   힌트: Service Account가 Earth Engine에 등록되었는지 확인하세요.")
    sys.exit(1)
except Exception as e:
    print(f"❌ 알 수 없는 에러: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# --- 3. Supabase 연결 ---
print("🔗 Supabase 연결 중...")

try:
    supabase = create_client(supabase_url, supabase_key)
    # 데이터가 있는지 확인
    test_query = supabase.table("oreum_metadata").select("id").limit(1).execute().data
    if not test_query:
        print("⚠️  오름 메타데이터 테이블이 비어있습니다.")
    else:
        print("✅ Supabase 연결 성공!")
except Exception as e:
    print(f"❌ Supabase 연결 오류: {e}")
    sys.exit(1)

# --- 4. 분석 시작 ---
print("🛰️ 위성 분석 시작...")

# 메타데이터 전체 가져오기
metadata = supabase.table("oreum_metadata").select("id, x_coord, y_coord").execute().data

if not metadata:
    print("☁️ 분석할 오름 데이터가 없습니다.")
    sys.exit(0)

print(f"📍 분석 대상: {len(metadata)}개 오름")

def add_all_indices(img):
    v = {'NIR': img.select('B8'), 'RED': img.select('B4'), 'BLUE': img.select('B2'), 
         'SWIR1': img.select('B11'), 'SWIR2': img.select('B12')}
    return img.addBands([
        img.normalizedDifference(['B3', 'B8']).rename('muddy_index'),
        img.expression('2.5 * ((NIR - RED) / (NIR + 6 * RED - 7.5 * BLUE + 1))', v).rename('green_visual_index'),
        img.expression('(NIR - (SWIR1 - SWIR2)) / (NIR + (SWIR1 - SWIR2))', v).rename('fire_risk_index'),
        img.expression('((SWIR1 + RED) - (NIR + BLUE)) / ((SWIR1 + RED) + (NIR + BLUE))', v).rename('erosion_index')
    ])

features = ee.FeatureCollection([
    ee.Feature(ee.Geometry.Point([m['x_coord'], m['y_coord']]), {'oreum_id': m['id']})
    for m in metadata
])

today = datetime.now()
today_str = today.strftime('%Y-%m-%d')
start_date = (today - timedelta(days=30)).strftime('%Y-%m-%d')

print(f"📅 분석 기간: {start_date} ~ {today_str}")

try:
    print("🔍 위성 이미지 수집 중...")
    latest_image = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
                    .filterDate(start_date, today_str)
                    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20))
                    .map(add_all_indices)
                    .median())

    print("📊 지수 계산 중...")
    results = latest_image.reduceRegions(
        collection=features, 
        reducer=ee.Reducer.mean(), 
        scale=10
    ).getInfo()

    data_dict = {}
    for f in results['features']:
        props = f['properties']
        o_id = props.get('oreum_id')
        
        # 값이 계산된 경우만 (None이 아닌 경우)
        if o_id and props.get('muddy_index') is not None:
            data_dict[o_id] = {
                "oreum_id": o_id, 
                "date": today_str,
                "muddy_index": props.get('muddy_index'),
                "green_visual_index": props.get('green_visual_index'),
                "fire_risk_index": props.get('fire_risk_index'),
                "erosion_index": props.get('erosion_index')
            }
            
    data_to_insert = list(data_dict.values())
    
    if data_to_insert:
        print(f"💾 데이터베이스 저장 중... ({len(data_to_insert)}건)")
        supabase.table("oreum_daily_stats").upsert(
            data_to_insert, 
            on_conflict="oreum_id, date"
        ).execute()
        print(f"🎉 성공! {len(data_to_insert)}건 저장 완료.")
        print(f"   저장된 날짜: {today_str}")
    else:
        print("☁️ 구름이 많거나 유효한 위성 데이터가 없습니다.")
        print("   (지난 30일간 구름 20% 미만 이미지를 찾을 수 없음)")

except ee.EEException as e:
    print(f"❌ Earth Engine 분석 중 에러: {e}")
    print("   힌트: 이미지 컬렉션이나 날짜 범위를 확인하세요.")
    sys.exit(1)
except Exception as e:
    print(f"❌ 분석 중 에러: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n✅ 모든 작업 완료!")