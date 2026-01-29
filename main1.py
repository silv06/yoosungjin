import os
import sys

print("--- 🔍 파이썬 환경 변수 정밀 진단 시작 ---")

# 1. 현재 파이썬이 볼 수 있는 모든 변수 이름(Key)만 출력
# (보안을 위해 값은 출력하지 않고 이름만 봅니다)
env_keys = list(os.environ.keys())
print(f"현재 감지된 변수 개수: {len(env_keys)}개")
print(f"변수 목록: {env_keys}")

print("-" * 30)

# 2. 우리가 찾는 그 녀석이 있는지 확인
target_key = 'GEE_SERVICE_ACCOUNT_KEY'

if target_key in os.environ:
    value = os.environ[target_key]
    print(f"✅ 찾았다! '{target_key}'가 존재합니다.")
    print(f"   - 데이터 길이: {len(value)} 글자")
    print(f"   - 첫 5글자: {value[:5]}")
    print(f"   - 마지막 5글자: {value[-5:]}")
    
    # 내용이 비어있는지 체크
    if not value or value.strip() == "":
        print("❌ [문제 발견] 변수는 있는데 내용이 '빈칸(Empty)'입니다!")
    else:
        print("🆗 내용도 들어있습니다. 이제 JSON 변환을 시도합니다...")
        import json
        try:
            json.loads(value)
            print("🎉 [최종 판정] JSON 형식이 완벽합니다. 인증이 가능합니다.")
        except json.JSONDecodeError as e:
            print(f"❌ [문제 발견] 내용은 있는데 'JSON 형식'이 아닙니다.")
            print(f"   - 에러 내용: {e}")
            print("   - 힌트: 복사할 때 괄호 {} 가 잘렸거나 이상한 글자가 섞였습니다.")

else:
    print(f"❌ [문제 발견] '{target_key}'가 변수 목록에 아예 없습니다.")
    print("👉 결론: 파이썬 코드가 실행되기 전에, 누군가 열쇠를 뺏어갔거나 안 줬습니다.")
    print("   (YAML 파일의 env 설정이 100% 원인입니다)")

print("--- 진단 종료 ---")
# 진단만 하고 프로그램 종료 (더 이상 에러 안 나게)
sys.exit(0)