import json
from datetime import datetime
from zoneinfo import ZoneInfo
import boto3
from utils.air_config import settings


def save_raw_to_s3(
    raw_data: dict,
):
    """
    실시간 대기질 API 원본 데이터를 S3에 저장
    """

    # S3 client 생성
    s3 = boto3.client(
        "s3",
        region_name=settings.AWS_REGION,
    )

    # 측정 시각 기준 파티셔닝
    data_time = raw_data["response"]["body"]["items"][0].get("dataTime")
    data_time_nm = data_time.replace(":", "-").replace(" ", "_")
    run_ts = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y%m%d_%H%M%S")

    key = (
        f"air-api/raw/real-time/"
        f"dataTime={data_time_nm}/"
        f"{run_ts}.json"
    )

    s3.put_object(
        Bucket=settings.AWS_S3_BUCKET,
        Key=key,
        Body=json.dumps(raw_data, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )

    return key
