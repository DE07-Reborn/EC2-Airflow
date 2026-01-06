# EC2-Airflow

**EC2-Airflow** 레포지토리는 AWS EC2 환경에서  
**데이터 수집 및 파이프라인 오케스트레이션을 담당하는 Apache Airflow 환경**을 구성하기 위한 설정을 포함합니다.

Airflow는 전체 시스템에서 다양한 외부 API 및 내부 데이터 소스를 주기적으로 수집하고,  
수집된 데이터를 **S3 적재 또는 Kafka Topic으로 전달**하여  
후속 Spark Streaming 및 추천 시스템 파이프라인이 원활히 동작하도록 합니다.

---

## Architecture Overview

- **역할**
  - 외부 API 기반 데이터 수집 스케줄링
  - Kafka Producer 역할 수행
  - S3 기반 장기 데이터 적재
  - 파이프라인 간 실행 순서 및 의존성 관리
- **배포 환경**
  - AWS EC2 (Private Subnet)
  - Docker & Docker Compose 기반 실행
- **연동 대상**
  - Kafka (Producer)
  - Spark Streaming
  - AWS S3
  - RDS (PostgreSQL)
  - 외부 Open API (KMA, Aladdin, Spotify 등)
 
---

## Prerequisites

- Amazon Linux 기반 EC2
- 보안 그룹에서 내부 서비스(Postgres, Kafka) 통신 허용
- Docker 및 Docker Compose 실행 권한
- AWS S3 접근을 위한 IAM Role 또는 Credential 설정

---

## 실행 방법

### 1. EC2 내부에서 Git 설치 및 레포지토리 클론

```bash
sudo yum update -y
sudo yum install -y git

git clone https://github.com/DE07-Reborn/EC2-Airflow.git
cd EC2-airflow
```

### 2. Docker 설치 및 서비스 활성화

```bash
sudo yum install -y docker
sudo systemctl start docker
sudo systemctl enable docker
```

### 3. Docker Compose 설치 

```bash
sudo mkdir -p /usr/local/lib/docker/cli-plugins

sudo curl -SL \
  https://github.com/docker/compose/releases/download/v2.27.0/docker-compose-linux-x86_64 \
  -o /usr/local/lib/docker/cli-plugins/docker-compose

sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
```

### 4. Airflow Scheduler | init 및 Postgres 컨테이너 실행

```bash
sudo docker compose up -d --build
```

---

## 컨테이너 구성 설명

| 컨테이너              | 역할                |
| ----------------- | ----------------- |
| postgres          | Airflow 메타데이터 저장  |
| airflow-scheduler | DAG 스케줄링 및 태스크 실행 |
| airflow-init      | DB 마이그레이션 및 초기 설정 |

---

## Airflow 오케스트레이션 역할
Airflow는 다음과 같은 데이터 수집 및 전달 작업을 수행합니다

### 기상 관측 지점 메타데이터
- **주기**: 6개월 단위
- **내용**: 기상 관측 지점(STN) 정보 수집
- **처리**: S3 적재
- **활용**: 유저 위치 기반 실시간 날씨 정보에 기준점으로 사용

### 베스트셀러 도서 데이터
- **주기**: 일주일(7일) 단위
- **내용**: 주간 베스트셀러 수집 및 전처리
- **처리**: S3 적재
- **활용**: 날씨 기반 도서 추천 로직에 사용

### 기상청 실시간 날씨 데이터
- **주기**: 05분 기준 매 1시간
- **내용**: 각 관측소에 대한 실시간 날씨 정보 수집
- **처리**: Kafka Producer를 통해 Topic으로 전송
- **활용**: 실시간 날씨 정보를 통해 노래/도서 추천 로직 구현 및 APP에서 사용

### 공공데이터 초단기예보 데이터
- **주기**: 정각 기준 30분 단위
- **내용**: 위경도 기반 향후 6시간 초단기 예보 수집
- **처리**: Kafka Producer를 통해 Topic으로 전송
- **활용**: 옷 추천 로직 및 우산 여부를 결정하는 데 사용

### 대기질 실시간 및 예보 데이터
- **주기**: 매 45분 단위 및 특정 시간대 
- **내용**: 위경도 기반 실시간 및 예보성 대기질 데이터 수집
- **처리**: Kafka Producer를 통해 Topic으로 전송
- **활용**: 미세먼지 상태 및 마스크 여부를 결정하는 데 사용

---

## 운영 참고 사항
- airflow 서버는 단일 Scheduler 중심 단일 노드 구성을 기준으로 설계되었습니다.
- 모든 DAG는 기본적으로 비활성화 상태로 생성되며, 필요 시 수동으로 활성화 해야 합니다.
- Private Subnet 내부 통신을 전제로 하며, 외부 접근은 Bastion 또는 Proxy를 통해 수행합니다.
- 로그 확인 :
  ```bash sudo docker compose logs -f <container-name>```
- 수동 활성화
  ### Docker 컨테이너로 이동 (bash)
  ```bash
  sudo docker exec -it airflow-scheduler bash
  ```

  ### Airflow 관련 동작
  ```bash
  # DAG 목록 확인
  airflow dags list
  
  # DAG 활성화
  airflow dags unpause <dag_id>
  
  # DAG 수동 실행
  airflow dags trigger <dag_id>
  
  # DAG 실행 결과 확인
  airflow dags list-runs -e <dag_id>
  ```
  





  
