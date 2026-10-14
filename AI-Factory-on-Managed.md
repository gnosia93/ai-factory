# AWS 관리형 서비스 기반 AI Factory 설계 정리

> 작성일: 2026-10-14
> 범위: Kubernetes를 직접 운영하지 않고 **Amazon SageMaker AI + Amazon Bedrock** 중심의 관리형 서비스로 AI Factory(데이터 → 학습/파인튜닝 → 배포/추론 → 피드백 루프)를 구성하는 방안
> 관련 문서: [AI-Factory-on-EKS.md](./AI-Factory-on-EKS.md) (EKS 기반 구성)

---

## 목차

1. [개요](#1-개요)
2. [관리형 기반 전체 아키텍처](#2-관리형-기반-전체-아키텍처)
3. [목적별 구성 패턴](#3-목적별-구성-패턴)
4. [구축 순서](#4-구축-순서)
5. [학습된 모델 저장 전략](#5-학습된-모델-저장-전략)
6. [LLM 서빙 콜드 스타트 및 스케일링](#6-llm-서빙-콜드-스타트-및-스케일링)
7. [파이프라인 도구 선택](#7-파이프라인-도구-선택)
8. [EKS 기반 vs 관리형 기반 비교](#8-eks-기반-vs-관리형-기반-비교)
9. [주의사항 요약](#9-주의사항-요약)
10. [다음 단계 (미결정 사항)](#10-다음-단계-미결정-사항)

---

## 1. 개요

관리형 구성의 목표는 **인프라 운영을 AWS에 맡기고 팀은 데이터와 모델에 집중하는 것**입니다.

- **Amazon Bedrock**: 파운데이션 모델(Claude 등)을 API로 쓰고, RAG, 에이전트, 가드레일, 커스터마이징을 제공
- **Amazon SageMaker AI**: 커스텀 모델 학습, 파인튜닝, 배포, MLOps
- **SageMaker HyperPod**: 대규모 분산 학습용 관리형 클러스터 (노드 장애 자동 복구)
- 클러스터, 노드, 드라이버, 오토스케일러를 직접 운영하지 않음

> 참고: 2024년 말부터 "Amazon SageMaker"는 데이터·분석·AI 통합 브랜드(Unified Studio)가 되었고, 기존 ML 기능은 **SageMaker AI**라는 이름으로 제공됩니다.

---

## 2. 관리형 기반 전체 아키텍처

```
┌─────────────────────────── 개발/협업 ───────────────────────────┐
│ SageMaker Unified Studio / SageMaker AI Studio (노트북, IDE)     │
└──────────────────────────────────────────────────────────────────┘
        │
┌────────────── 데이터 ──────────────┐   ┌──────── 파운데이션 모델 ────────┐
│ S3 데이터 레이크                    │   │ Amazon Bedrock                   │
│ Glue (ETL, Catalog), Lake Formation │   │  ├ 모델 API (On-Demand/Batch)    │
│ Athena, EMR Serverless              │   │  ├ Knowledge Bases (RAG)         │
│ SageMaker Feature Store             │   │  ├ Agents / AgentCore            │
│ SageMaker Ground Truth (라벨링)     │   │  ├ Guardrails                    │
└─────────────────────────────────────┘   │  ├ Model Customization(파인튜닝)│
        │                                 │  ├ Custom Model Import           │
        ▼                                 │  └ Model Evaluation              │
┌────────────── 학습 ────────────────┐   └──────────────────────────────────┘
│ SageMaker Training Jobs (단일/분산) │
│ SageMaker HyperPod (대규모, 장기)   │
│  └ 오케스트레이터: Slurm 또는 EKS   │
│ Managed Spot Training               │
│ Training Plans (GPU 용량 예약)      │
└─────────────────────────────────────┘
        │
        ▼
┌────────────── MLOps ───────────────┐
│ SageMaker Pipelines                 │
│ SageMaker Managed MLflow (실험 추적)│
│ SageMaker Model Registry            │
│ SageMaker Model Monitor / Clarify   │
└─────────────────────────────────────┘
        │
        ▼
┌────────────── 추론 ────────────────┐
│ SageMaker Real-time Endpoints       │
│  └ Inference Components (멀티 모델) │
│ SageMaker Async / Batch Transform   │
│ Bedrock (Custom Model Import /      │
│          Provisioned Throughput)    │
│ API Gateway + Lambda (앱 연동)      │
└─────────────────────────────────────┘

보안   : IAM, KMS, VPC 모드(학습/엔드포인트), PrivateLink, Secrets Manager
거버넌스: Control Tower, CloudTrail, SageMaker Model Cards, Bedrock Guardrails
관측/비용: CloudWatch, Cost Explorer, Savings Plans(SageMaker), 태그 기반 비용 배분
```

### 핵심 구성 요소

| 영역 | 서비스 | 역할 |
|---|---|---|
| 데이터 | S3, Glue, Lake Formation, Athena | 데이터 레이크, ETL, 권한 관리 |
| 피처 | SageMaker Feature Store | 학습과 추론에서 같은 피처를 일관되게 사용 |
| 라벨링 | SageMaker Ground Truth | 사람이 검토하는 라벨링, RLHF 데이터 수집 |
| 학습 | SageMaker Training Jobs | 작업 단위로 실행하고 끝나면 리소스 자동 반납 |
| 대규모 학습 | SageMaker HyperPod | 장기 실행 클러스터, 장애 노드 자동 교체, 체크포인트 재개 |
| FM 커스터마이징 | Bedrock Model Customization | 코드 없이 파인튜닝, 증류(distillation) |
| 실험 추적 | SageMaker Managed MLflow | MLflow 서버를 직접 운영하지 않음 |
| 레지스트리 | SageMaker Model Registry | 모델 버전, 승인 상태(Approved/Rejected) |
| 파이프라인 | SageMaker Pipelines | 학습 → 평가 → 등록 → 배포 자동화 |
| 서빙 | SageMaker Endpoints, Inference Components | 오토스케일, 한 엔드포인트에 여러 모델 탑재 |
| FM 서빙 | Bedrock | 서버리스 API, 커스텀 모델 가져오기 |
| 모니터링 | Model Monitor, Clarify | 데이터 드리프트, 편향, 설명 가능성 |
| RAG/에이전트 | Bedrock Knowledge Bases, Agents, AgentCore | 벡터 인덱싱, 도구 호출, 에이전트 런타임 |

---

## 3. 목적별 구성 패턴

관리형 구성은 **목적에 따라 쓰는 서비스가 크게 달라집니다.**

### A. 기존 FM 활용 (RAG / 에이전트): 가장 빠르고 저렴

```
문서(S3) → Bedrock Knowledge Bases(임베딩, 벡터 저장: OpenSearch Serverless / S3 Vectors / Aurora pgvector)
앱 → API Gateway/Lambda → Bedrock Agents(또는 AgentCore) → Claude 등 FM + Guardrails
```

- GPU, 학습 인프라가 필요 없음
- 비용은 토큰 단위 과금, 트래픽이 크면 Provisioned Throughput 검토

### B. 파인튜닝

| 방식 | 사용 시점 |
|---|---|
| Bedrock Model Customization | 지원되는 모델을 코드 없이 파인튜닝하거나 증류할 때 |
| SageMaker JumpStart | 오픈 모델(Llama, Qwen, Mistral 등)을 클릭이나 SDK로 파인튜닝·배포할 때 |
| SageMaker Training Jobs | 직접 작성한 학습 코드(LoRA, QLoRA, 전체 파인튜닝)를 돌릴 때 |

### C. 대규모 사전학습 / 장기 학습

- **SageMaker HyperPod**: 수십~수백 노드(p5/p6, trn2), EFA, FSx for Lustre
- 장애 노드 자동 감지·교체, 체크포인트에서 자동 재개
- 오케스트레이터는 Slurm 또는 EKS 중 선택 (EKS를 쓰면 EKS 기반 구성과 연결 가능)
- **Training Plans**로 GPU 용량을 미리 예약

---

## 4. 구축 순서

1. **랜딩존**: Control Tower로 멀티 계정(data / ml-dev / ml-prod / shared), VPC·IAM 기준선
2. **데이터 레이크**: S3 + Glue Catalog + Lake Formation
3. **SageMaker 도메인**: Studio 도메인(VPC 전용 모드), 사용자 프로필과 실행 역할, Managed MLflow 서버
4. **목적별 서비스 활성화**
   - A(RAG/에이전트): Bedrock 모델 접근 권한, Knowledge Base, Guardrails
   - B(파인튜닝): JumpStart 또는 Training Job 템플릿
   - C(대규모 학습): HyperPod 클러스터, FSx for Lustre, Training Plans
5. **MLOps 파이프라인**: SageMaker Pipelines(학습 → 평가 → 조건 분기 → Model Registry 등록)
6. **배포 자동화**: Model Registry 승인 이벤트 → EventBridge → CodePipeline → 엔드포인트 배포 (dev → prod 계정)
7. **서빙**: Inference Components, 오토스케일, Shadow Test, Blue/Green 배포
8. **모니터링**: Model Monitor(드리프트), CloudWatch 알람, Bedrock 호출 로깅
9. **피드백 루프**: 추론 데이터 캡처(S3) → Ground Truth로 검토·라벨링 → 재학습

### 저장소 골격 (예정)

```
ai-factory-managed/
├── terraform/          # 랜딩존 기준선, VPC, S3, KMS, SageMaker Domain, MLflow, Bedrock 설정
├── pipelines/          # SageMaker Pipelines 정의 (Python SDK)
├── training/           # 학습 스크립트, 컨테이너 (필요할 때만 BYOC)
├── inference/          # 엔드포인트 설정, Inference Component 정의
├── rag/                # Knowledge Base, Agent, Guardrail 설정
└── cicd/               # 모델 승인 후 배포 워크플로
```

---

## 5. 학습된 모델 저장 전략

EKS 구성과 원칙은 같습니다. **최종 저장소는 S3이고 레지스트리가 버전을 관리합니다.** 다만 관리형 서비스가 이 흐름을 자동으로 처리합니다.

### 저장 흐름

```
[SageMaker Training Job]
   ├─ 체크포인트: /opt/ml/checkpoints → S3 자동 동기화 (Spot 중단 시 재개용)
   └─ 최종 모델 : /opt/ml/model → 작업 종료 시 S3에 model.tar.gz로 자동 업로드
                   (LLM은 압축하지 않은 S3 prefix 방식 권장: 로딩이 빠름)
        │
        ▼
[SageMaker Model Registry]  ── Model Package Group / 버전 / 승인 상태 / 지표
[Managed MLflow]            ── 실험 지표, 파라미터, 아티팩트 링크
        │
        ▼
[배포] SageMaker Endpoint 또는 Bedrock Custom Model Import가 S3에서 로딩

[HyperPod] 체크포인트 → FSx for Lustre → S3 (Data Repository Association)
[Bedrock 파인튜닝] 커스텀 모델은 Bedrock 내부에 저장되며 계정 전용으로 관리됨
```

### S3 버킷 구조

```
s3://ai-factory-models/
├── checkpoints/{job_name}/                  # Training Job 체크포인트 (Lifecycle으로 정리)
├── training-output/{job_name}/output/       # Training Job 결과 (model.tar.gz)
├── models/{model_name}/{version}/           # 압축하지 않은 LLM 가중치 (safetensors)
└── releases/{model_name}/{version}/         # 운영 승인 모델 (Object Lock)
```

### 운영 팁

- **LLM은 압축하지 않은 상태로 저장**: `model.tar.gz`는 해제 시간이 오래 걸림. `ModelDataSource`에 `CompressionType: None`인 S3 prefix를 지정
- **레지스트리 승인 상태**로 배포를 게이팅: `PendingManualApproval` → `Approved` 이벤트로 배포 트리거
- **계정 간 공유**: Model Registry와 S3 버킷 정책, KMS 키 정책으로 dev에서 학습하고 prod에서 배포
- **Model Cards**로 모델 용도, 학습 데이터, 평가 결과를 문서화 (거버넌스)

---

## 6. LLM 서빙 콜드 스타트 및 스케일링

관리형 서비스는 콜드 스타트 최적화 기능을 **기본 제공하거나 옵션으로 켤 수 있다**는 점이 EKS와 가장 큰 차이입니다.

### 6.1 서빙 옵션별 특징

| 옵션 | 콜드 스타트 | 스케일링 | 적합한 경우 |
|---|---|---|---|
| **Bedrock On-Demand** (기본 FM) | 없음 (AWS가 상시 운영) | 자동 (계정 쿼터 범위 내) | 지원되는 FM을 그대로 쓸 때 |
| **Bedrock Provisioned Throughput** | 없음 (전용 용량) | 구매한 Model Unit만큼 고정 | 처리량 보장이 필요할 때 |
| **Bedrock Custom Model Import** | 첫 호출이나 유휴 후 지연 가능 | 자동 (서버리스) | 지원 아키텍처의 오픈 모델을 서버리스로 쓸 때 |
| **SageMaker Real-time Endpoint** | 인스턴스 추가 시 수 분 (최적화 기능으로 단축) | Application Auto Scaling | 커스텀 모델, 세밀한 제어 |
| **SageMaker Inference Components** | 컴포넌트 단위로 확장 (0까지 축소 가능) | 모델별 독립 스케일 | 한 엔드포인트에 여러 모델, 비용 최적화 |
| **SageMaker Async Inference** | 허용 (큐 기반) | 0까지 축소 가능 | 긴 요청, 배치성 트래픽 |

### 6.2 SageMaker 콜드 스타트 최적화 기능

| 단계 | EKS에서 직접 하던 것 | SageMaker 관리형 기능 |
|---|---|---|
| ① 인스턴스 기동 | Karpenter, 웜 풀 | 최소 인스턴스 수 유지, 예약 용량 |
| ② 컨테이너 이미지 | EBS 스냅샷, SOCI | **Container Caching**: 스케일아웃 시 이미지 pull 생략 |
| ③ 모델 다운로드 | Run:ai Streamer, NVMe, FSx | **Fast Model Loader**: S3에서 GPU로 직접 스트리밍, 미리 샤딩한 가중치 사용 |
| ④ 워밍업 | 컴파일 캐시 직접 관리 | **Inference Optimization Toolkit**: 양자화, 컴파일, 투기적 디코딩을 미리 적용 |

> 위 기능의 지원 인스턴스, 컨테이너(LMI/DJL, TGI 등), 리전은 계속 바뀌므로 적용 전에 최신 문서를 확인합니다.

### 6.3 스케일링 전략

| 전략 | 내용 |
|---|---|
| 지표 기반 오토스케일 | `ConcurrentRequestsPerModel`(고해상도 지표), `InvocationsPerInstance`, GPU 사용률 |
| 여유 있는 Target Tracking | 목표값을 낮게 잡아 피크 전에 확장 |
| Scheduled Scaling | 예측 가능한 피크는 시간 기반으로 미리 확장 |
| 최소 인스턴스 유지 | 핵심 모델은 `MinCapacity >= 1` |
| Inference Components | 모델별 복제본 수를 독립적으로 조절, 비핵심 모델은 0까지 축소 |
| 완만한 축소 | scale-in cooldown을 길게 설정 |

### 6.4 권장 조합

```
기존 FM으로 충분 : Bedrock On-Demand → 트래픽이 커지면 Provisioned Throughput
운영 핵심 커스텀 : SageMaker Endpoint(MinCapacity ≥ 1) + Container Caching + Fast Model Loader
                  + 동시 요청 수 기반 오토스케일
다수의 비핵심 모델: Inference Components로 한 엔드포인트에 탑재, 0까지 축소 허용
배치/비동기      : Async Inference(0까지 축소) 또는 Bedrock Batch Inference
```

---

## 7. 파이프라인 도구 선택

관리형 구성에서는 **SageMaker Pipelines를 기본**으로 하고, 필요하면 다른 서비스와 조합합니다.

### 7.1 비교

| | SageMaker Pipelines | AWS Step Functions | Amazon MWAA (Airflow) |
|---|---|---|---|
| 목적 | ML 파이프라인 전용 | 범용 서버리스 워크플로 | 데이터 ETL 스케줄링 |
| 정의 방식 | Python SDK (`@step` 데코레이터 지원) | JSON/YAML(ASL), Workflow Studio | Python DAG |
| ML 기능 | 학습/튜닝/평가/등록 스텝, 캐싱, 계보, Model Registry 연동 | SageMaker API 직접 호출 통합 | SageMaker 오퍼레이터 |
| 운영 부담 | 없음 (서버리스) | 없음 (서버리스) | 낮음 (환경 크기만 관리) |
| 비용 | 파이프라인 자체는 무료, 실행 리소스만 과금 | 상태 전환 횟수 과금 | 환경 상시 과금 |
| 강점 | MLflow, Registry, Studio UI와 통합 | 200개 이상 AWS 서비스와 통합, 사람 승인 단계 | 커넥터 생태계, backfill |

### 7.2 권장 조합

```
[MWAA 또는 Glue Workflows] ── 데이터 파이프라인: 수집 → 정제 → 학습 데이터셋 (S3)
        │  트리거 (EventBridge / StartPipelineExecution API)
        ▼
[SageMaker Pipelines] ──────── ML 파이프라인: 전처리 → 학습 → 평가 → 조건 → Registry 등록
        │  Registry 승인 이벤트 (EventBridge)
        ▼
[CodePipeline / Step Functions] ── 배포: dev 엔드포인트 → 테스트 → 사람 승인 → prod 배포
```

| 상황 | 추천 |
|---|---|
| ML 파이프라인 위주, 데이터 사이언티스트가 작성 | **SageMaker Pipelines** |
| 여러 AWS 서비스를 엮는 업무 흐름, 승인 단계 필요 | **Step Functions** |
| 이미 Airflow를 쓰거나 ETL 비중이 큼 | **MWAA** + SageMaker 오퍼레이터 (ML 부분은 Pipelines로 위임) |

---

## 8. EKS 기반 vs 관리형 기반 비교

| 항목 | EKS 기반 | 관리형 기반 (SageMaker AI + Bedrock) |
|---|---|---|
| 운영 부담 | 높음 (클러스터, 애드온, 드라이버, 업그레이드) | 낮음 |
| 유연성 | 매우 높음 (모든 OSS 도구, 커스텀 스케줄링) | 서비스가 제공하는 범위 안 |
| 콜드 스타트 최적화 | 직접 구현 (캐시, 웜 풀) | 기능을 켜서 사용 (Container Caching, Fast Model Loader) |
| GPU 활용률 | 높게 끌어올릴 수 있음 (bin-packing, 공유) | 엔드포인트·작업 단위라서 유휴가 생기기 쉬움 (Inference Components로 보완) |
| 비용 구조 | EC2 가격 + 운영 인력 | 인스턴스 가격에 관리형 프리미엄 포함, 운영 인력 절감 |
| 이식성 | 높음 (멀티 클라우드, 온프레미스로 이동 쉬움) | AWS 종속 |
| 초기 구축 속도 | 느림 (수 주~수 개월) | 빠름 (수 일~수 주) |
| 필요 역량 | Kubernetes, 플랫폼 엔지니어링 | ML 엔지니어링, AWS 서비스 이해 |

### 선택 기준

- **관리형이 맞는 경우**: 플랫폼 팀이 작거나 없음, 빠른 출시가 중요, 기존 FM 활용(RAG/에이전트)이 주목적, 모델 수가 많지 않음
- **EKS가 맞는 경우**: GPU 규모가 크고 활용률이 비용에 큰 영향을 줌, OSS 스택을 자유롭게 써야 함, 멀티 클라우드나 온프레미스 이식성이 필요함

### 하이브리드 (현실적인 절충안)

```
RAG/에이전트           → Bedrock (관리형)
실험 추적/레지스트리    → SageMaker Managed MLflow, Model Registry (관리형)
대규모 학습            → SageMaker HyperPod (EKS 오케스트레이터) ← 두 방식의 접점
대량 추론 (비용 민감)   → EKS + vLLM
소량/다수 모델 추론     → SageMaker Inference Components
```

처음에는 관리형으로 빠르게 시작하고, GPU 비용이 커지는 영역만 EKS로 옮기는 방식이 일반적입니다.

---

## 9. 주의사항 요약

- **서비스 쿼터**: SageMaker 인스턴스 유형별 쿼터(학습/엔드포인트 각각)와 Bedrock 토큰/요청 쿼터를 미리 증설 요청
- **GPU 확보**: 대형 GPU는 Training Plans나 HyperPod 예약 용량으로 확보
- **비용**: 엔드포인트는 켜져 있는 동안 계속 과금됨. 비핵심 모델은 Inference Components의 0 축소, Async, Serverless 활용. SageMaker Savings Plans 검토
- **모델 포맷**: LLM은 압축하지 않은 S3 prefix로 저장 (tar.gz 해제 지연 방지)
- **보안**: SageMaker 도메인·학습·엔드포인트를 VPC 모드로, KMS 암호화, Bedrock은 PrivateLink. Bedrock은 고객 입력을 모델 학습에 사용하지 않음
- **거버넌스**: Bedrock 호출 로깅(CloudWatch/S3), Guardrails, Model Cards, 계정 간 모델 승인 프로세스
- **종속성**: 관리형 API에 깊게 묶이면 나중에 이전 비용이 큼. 학습 코드와 모델 포맷은 표준(PyTorch, Hugging Face)으로 유지

---

## 10. 다음 단계 (미결정 사항)

- [ ] 리전 결정 (Bedrock 모델과 SageMaker 기능 지원 여부 확인)
- [ ] 주 목적 결정: A(RAG/에이전트), B(파인튜닝), C(대규모 학습)
- [ ] 관리형 단독 vs 하이브리드(EKS 병행) 결정
- [ ] 기존 Airflow(MWAA) 사용 여부 확인
- [ ] IaC 도구 확정 (Terraform 또는 CDK)
- [ ] 서비스 쿼터 증설 요청 목록 작성
