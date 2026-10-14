# AWS EKS 기반 AI Factory 설계 정리

> 작성일: 2026-10-14
> 범위: AWS 클라우드 위에 EKS를 중심으로 AI Factory(데이터 → 학습/파인튜닝 → 배포/추론 → 피드백 루프)를 구성하는 방안

---

## 목차

1. [개요](#1-개요)
2. [EKS 기반 전체 아키텍처](#2-eks-기반-전체-아키텍처)
3. [구축 순서](#3-구축-순서)
4. [학습된 모델 저장 전략](#4-학습된-모델-저장-전략)
5. [LLM 서빙 콜드 스타트 및 스케일링](#5-llm-서빙-콜드-스타트-및-스케일링)
6. [파이프라인 도구 선택: Airflow vs Argo vs Kubeflow](#6-파이프라인-도구-선택-airflow-vs-argo-vs-kubeflow)
7. [주의사항 요약](#7-주의사항-요약)
8. [다음 단계 (미결정 사항)](#8-다음-단계-미결정-사항)

---

## 1. 개요

**AI Factory**는 데이터 수집부터 학습, 배포, 운영 피드백까지가 반복적으로 돌아가는 AI 생산 플랫폼입니다.

- 참고: AWS에는 고객 데이터센터에 전용 AI 인프라(Trainium/NVIDIA GPU)를 설치해 주는 **"AWS AI Factories"** 상품도 있습니다. 온프레미스나 데이터 주권 요구가 있으면 AWS 영업 쪽과 상담하는 방향을 고려합니다.
- 이 문서는 **AWS 클라우드 + EKS 중심** 구성을 다룹니다.
- 출발점으로 쓸 공식 레퍼런스: **AI on EKS** 블루프린트 (`awslabs/ai-on-eks`)

### 계층별 AWS 서비스 (일반)

| 계층 | 주요 서비스 |
|---|---|
| 데이터 | S3(데이터 레이크), Glue/EMR, Lake Formation, Athena |
| 학습 컴퓨팅 | EC2 P5/P6(NVIDIA), Trn2(Trainium), EKS + Karpenter |
| 고속 스토리지/네트워크 | FSx for Lustre, EFA, Placement Group / UltraCluster |
| 추론 | EKS + vLLM/Triton/KServe, Inferentia2(Inf2) |
| 파운데이션 모델 (선택) | Amazon Bedrock (RAG, Agents, Guardrails) |
| 거버넌스/보안 | IAM, KMS, PrivateLink, CloudTrail, Control Tower |
| 관측/비용 | CloudWatch, Managed Prometheus/Grafana, Kubecost, Capacity Blocks for ML |

---

## 2. EKS 기반 전체 아키텍처

학습, 파인튜닝, 서빙, 파이프라인을 **모두 컨테이너로 EKS 위에서 운영**합니다.

```
┌──────────────────────────── EKS Cluster ────────────────────────────┐
│ [GitOps] Argo CD ─ 모든 애드온/워크로드를 Git으로 관리                │
│                                                                       │
│ [파이프라인] Argo Workflows (+ Hera SDK) / Kubeflow Pipelines         │
│ [실험/레지스트리] MLflow (RDS + S3 백엔드)                            │
│ [분산 학습] Kubeflow Training Operator(PyTorchJob) / KubeRay          │
│ [스케줄링] Kueue (GPU 쿼터·큐잉·팀별 공정 분배)                       │
│ [서빙] vLLM / NVIDIA Triton / KServe / Ray Serve                      │
│ [게이트웨이] AWS LB Controller(ALB/NLB) + (선택) LLM Gateway           │
│ [관측] Prometheus + Grafana + DCGM Exporter(GPU 지표) + Fluent Bit    │
│                                                                       │
│ ── Node Pools (Karpenter) ──────────────────────────────────────────  │
│  system   : m7i/m7g (On-Demand) ─ 컨트롤러, 애드온                    │
│  train-gpu: p5/p5e/p6 (Capacity Block/ODCR) + EFA ─ 대규모 학습       │
│  train-trn: trn2 + EFA + Neuron ─ 비용 효율 학습                      │
│  infer-gpu: g6e/g6/p4d (On-Demand + Spot) ─ LLM 추론                  │
│  infer-inf: inf2 ─ 저비용 추론                                        │
└───────────────────────────────────────────────────────────────────────┘
   스토리지 : S3(데이터 레이크/모델 아티팩트), FSx for Lustre(학습 캐시, S3 연동),
              Mountpoint for S3 CSI(모델 로딩), EFS(공유 노트북)
   레지스트리: ECR (pull-through cache 포함)
   보안     : EKS Pod Identity, KMS, Secrets Manager + External Secrets, VPC Endpoints
```

### 핵심 구성 요소

| 영역 | 구성 | 비고 |
|---|---|---|
| 클러스터 | EKS(최신 버전), 프라이빗 엔드포인트, 3-AZ VPC | 학습 노드는 EFA 때문에 **단일 AZ 서브넷**에 배치 |
| 노드 오토스케일 | Karpenter | GPU 종류별 NodePool과 taint로 워크로드 분리 |
| GPU 드라이버 | EKS 가속 AMI(AL2023 NVIDIA/Neuron) + NVIDIA device plugin 또는 GPU Operator | Neuron/EFA device plugin 별도 설치 |
| 학습 | Training Operator, KubeRay, Kueue | NCCL + EFA, 체크포인트는 FSx에서 S3로 저장 |
| 서빙 | vLLM, Triton, KServe | KEDA로 큐 길이나 토큰 처리량 기반 스케일 |
| 파이프라인 | Argo Workflows 또는 KFP | 데이터 준비 → 학습 → 평가 → MLflow 등록 → 배포 |
| GitOps | Argo CD | dev, prod 클러스터를 같은 방식으로 관리 |
| 관측 | AMP/AMG + DCGM Exporter | GPU 사용률, 메모리, NCCL 통신량 |
| 비용 | Kubecost, Spot(추론/배치), Capacity Blocks(학습) | Karpenter consolidation으로 유휴 노드 정리 |

> **EKS Auto Mode**: 운영 부담을 줄이고 싶다면 Auto Mode도 GPU를 지원합니다. 다만 EFA나 세밀한 튜닝이 필요하면 Karpenter를 직접 운영하는 쪽이 유연합니다.

---

## 3. 구축 순서

1. **랜딩존**: Control Tower로 멀티 계정(data / ml-dev / ml-prod / shared) 구성, VPC·IAM 기준선 설정
2. **기반 인프라 (Terraform)**: VPC, EKS, Karpenter, ECR, S3, FSx for Lustre, RDS(MLflow용)
3. **클러스터 애드온**: LB Controller, EBS/FSx/Mountpoint CSI, device plugin(NVIDIA/Neuron/EFA), Prometheus 스택
4. **GitOps 전환**: Argo CD 설치 후 모든 애드온과 워크로드를 Helm/Kustomize로 Git에서 관리
5. **학습 스택**: Training Operator, KubeRay, Kueue 설치, PyTorchJob으로 멀티노드 EFA 학습 검증
6. **서빙 스택**: vLLM으로 오픈 모델 배포, ALB 노출, 오토스케일링 검증
7. **MLOps 루프**: MLflow + Argo Workflows로 학습 → 평가 → 등록 → 배포 자동화
8. **멀티 테넌시**: 팀별 Namespace, Kueue ClusterQueue, ResourceQuota, NetworkPolicy
9. **피드백 루프**: 추론 로그를 S3에 저장 → 평가 → 재학습 데이터로 활용

### 저장소 골격 (예정)

```
ai-factory/
├── terraform/      # VPC, EKS, Karpenter, FSx, S3, ECR, RDS
├── gitops/         # Argo CD App-of-Apps, 애드온 Helm values
├── nodepools/      # Karpenter NodePool/EC2NodeClass (GPU/Trn/Inf)
└── examples/       # PyTorchJob(EFA), vLLM 서빙, Argo 파이프라인 샘플
```

---

## 4. 학습된 모델 저장 전략

**최종 저장소는 S3이고, MLflow Model Registry가 그 버전을 관리합니다.** 학습 중에는 빠른 FSx for Lustre에 먼저 쓰고, 이 내용이 S3로 동기화됩니다.

### 저장 흐름

```
[학습 Pod] ──체크포인트(주기적)──▶ FSx for Lustre (/fsx/checkpoints)
                                     │  Data Repository Association(자동 동기화)
                                     ▼
                          S3: s3://ai-factory-models/checkpoints/...
[학습 완료] ──최종 모델 + 메타데이터──▶ MLflow 등록
                          ├─ 메타데이터(버전, 지표, 태그) → RDS(PostgreSQL)
                          └─ 모델 파일(safetensors 등)    → S3: .../registry/...
[서빙 Pod] ◀──로딩── S3 (캐시 계층 경유, 5장 참고)
```

### 저장소별 역할

| 저장소 | 저장 대상 | 이유 |
|---|---|---|
| FSx for Lustre | 학습 중 체크포인트 | 멀티노드가 동시에 대용량을 쓸 때 S3보다 훨씬 빠름 |
| **S3 (메인)** | 체크포인트 백업, 최종 모델 가중치 | 저렴하고 내구성 높음. Versioning과 KMS 적용 |
| MLflow Registry | 모델 버전, 지표, 스테이지 | 운영 중인 모델 추적. 실제 파일은 S3를 가리킴 |
| ECR (선택) | 서빙 이미지, OCI 아티팩트 | 작은 모델을 이미지째 배포하거나 버전을 고정할 때 |

### S3 버킷 구조

```
s3://ai-factory-models/
├── checkpoints/{project}/{run_id}/step-{N}/   # 중간 체크포인트 (Lifecycle: 30일 후 삭제/Glacier)
├── registry/{model_name}/{version}/           # MLflow 관리 최종 모델
│   ├── model.safetensors
│   ├── config.json
│   └── tokenizer.json
└── releases/{model_name}/{version}/           # 운영 승인 모델 (Object Lock, 변경 불가)
```

### 운영 팁

- 버킷: Versioning, KMS 암호화, VPC Endpoint 전용 접근. `releases/`에는 Object Lock 적용
- 비용: 중간 체크포인트는 S3 Lifecycle으로 정리
- 권한: EKS Pod Identity로 학습 Pod에는 쓰기, 서빙 Pod에는 읽기 권한만 부여
- 포맷: LLM은 Hugging Face 형식(`safetensors`)으로 저장해 vLLM이나 Triton에서 바로 로딩

---

## 5. LLM 서빙 콜드 스타트 및 스케일링

S3는 **원본 저장소(Single Source of Truth)**로만 쓰고, 서빙할 때는 **캐시 계층과 선제적 스케일링**을 함께 설계합니다.

### 5.1 콜드 스타트 시간 구성 (70B FP16, 약 140GB 기준, 대략적인 추정치)

| 단계 | 최적화 전 | 최적화 후 |
|---|---|---|
| ① GPU 노드 프로비저닝 | 1~3분 | 0초 (웜 노드) |
| ② 컨테이너 이미지 pull (10GB 이상) | 2~5분 | 수 초 (스냅샷 프리캐시) |
| ③ 모델 다운로드 (S3 → 노드) | 단일 스트림이면 10분 이상 | 30초~1분 |
| ④ GPU 로딩 + 워밍업 | 1~3분 | 30초 내외 |
| **합계** | **15~20분 이상** | **1~2분, 또는 사실상 0 (웜 풀)** |

### 5.2 단계별 해결책

**① 노드 기동**
- Overprovisioning: 우선순위가 낮은 placeholder Pod로 GPU 노드를 미리 확보
- 핵심 모델은 `minReplicas >= 1`, 0까지 축소는 비핵심 모델에만 적용

**② 컨테이너 이미지**
- 이미지를 미리 받아 둔 EBS 스냅샷을 Karpenter `EC2NodeClass.blockDeviceMappings.snapshotID`로 연결
- **Fast Snapshot Restore(FSR)**를 반드시 켬 (켜지 않으면 첫 읽기가 지연 로딩되어 느림)
- 대안: SOCI 지연 로딩

**③ 모델 로딩: 3단계 캐시**

```
[L1] 노드 로컬 NVMe (instance store) ── 가장 빠름, DaemonSet이 노드 기동 시 프리페치
[L2] FSx for Lustre (S3 연결)  ──────── 모든 노드가 공유, 처리량이 높아 스케일아웃에 유리
[L0] S3 (원본, 버전 관리)       ──────── 단일 진실 원천
```

| 방식 | 특징 | 적합한 경우 |
|---|---|---|
| S3 병렬 스트리밍 (vLLM `--load-format runai_streamer`, s5cmd) | S3에서 GPU로 병렬 스트리밍 | 가장 간단한 1차 개선 |
| 노드 로컬 NVMe 캐시 | 노드당 한 번만 받고 재사용 | Pod 재시작이나 롤링 업데이트가 잦을 때 |
| FSx for Lustre | 동시에 읽어도 처리량 유지 (용량 비례, 비용 있음) | 스케일아웃이 잦고 모델이 클 때 |
| 모델을 넣은 EBS 스냅샷 (FSR 필수) | 노드가 뜨면 모델이 이미 있음 | 운영 모델이 고정되어 있을 때 |

**④ 모델 경량화**
- 양자화(FP8, AWQ/GPTQ): 크기를 1/2~1/4로 줄임 (70B FP8은 약 70GB)
- 샤딩된 safetensors와 텐서 병렬: GPU마다 자기 샤드를 병렬 로딩
- torch.compile이나 TensorRT-LLM 엔진을 미리 빌드해 캐시

### 5.3 스케일링 전략

| 전략 | 내용 |
|---|---|
| 선행 지표 기반 오토스케일 | vLLM `num_requests_waiting`, KV cache 사용률, TTFT를 KEDA + Prometheus로 연결 |
| 여유 있는 임계값 | KV cache 70% 정도에서 확장 시작 |
| 스케줄 기반 사전 확장 | 예측 가능한 피크는 KEDA Cron으로 미리 확장 |
| 웜 풀 | placeholder Pod로 GPU 노드 1~2대 상시 대기 |
| 완만한 축소 | scale-down 안정화 시간 10~30분 |
| 모델별 분리 | 모델마다 Deployment와 NodePool 분리 |

### 5.4 권장 조합

```
운영 핵심 모델  : minReplicas ≥ 1 + 웜 노드 1대 + 로컬 NVMe 캐시 + KEDA(큐 기반)
                 → 스케일아웃 시 신규 Pod 기동 1분 내외
비핵심/실험 모델: 0까지 축소 허용 + Run:ai Streamer로 S3 직접 로딩
                 → 콜드 스타트 수 분 허용, 비용 최소화
대규모 스케일아웃: FSx for Lustre 공유 캐시 추가
```

**도입 순서**: Run:ai Streamer와 웜 풀부터 적용 → 측정 → 부족하면 NVMe 캐시나 FSx 추가

---

## 6. 파이프라인 도구 선택: Airflow vs Argo vs Kubeflow

Airflow를 배제하는 것은 아닙니다. **EKS 중심 구성이라서 Kubernetes 네이티브 도구를 기본값으로 제안**한 것입니다.

### 6.1 비교

| | Airflow | Argo Workflows | Kubeflow Pipelines |
|---|---|---|---|
| 원래 목적 | 데이터 ETL 배치 스케줄링 | Kubernetes 범용 워크플로 | ML 파이프라인 전용 |
| 정의 방식 | Python DAG | YAML(CRD), Hera(Python) | Python SDK |
| 실행 단위 | Task (Pod 실행은 KubernetesPodOperator/Executor 필요) | 각 단계가 그대로 Pod | 각 단계가 Pod (내부적으로 Argo 기반이 일반적) |
| 운영 부담 | Scheduler, Webserver, DB, Worker | 컨트롤러 하나 (가벼움) | 상대적으로 무거움 |
| AWS 관리형 | MWAA | 없음 | 없음 |

### 6.2 Argo/Kubeflow가 EKS에서 유리한 점

1. **Kubernetes 리소스를 직접 지정**: GPU, EFA, nodeSelector, toleration을 Pod 스펙 그대로 사용하고 Karpenter, Kueue와 자연스럽게 연동
2. **GitOps 일관성**: Workflow가 CRD라서 Argo CD로 함께 배포하고 버전 관리
3. **대규모 병렬과 아티팩트 전달**: 수백~수천 Pod fan-out, 단계 간 S3 아티팩트 전달 (Airflow의 XCom은 작은 메타데이터용)
4. **ML 전용 기능 (KFP)**: 아티팩트 계보, 단계 캐싱, 실행 비교 UI
5. **이벤트 기반 트리거**: Argo Events로 S3 업로드나 SQS 메시지가 오면 재학습

### 6.3 Airflow가 더 나은 경우

- DB, 데이터 웨어하우스, SaaS 커넥터가 압도적으로 많음
- 시간 기반 배치 스케줄링 기능(backfill, catchup, SLA)이 성숙함
- 관리형 서비스(MWAA)로 운영 부담이 없음
- 데이터 엔지니어들이 이미 익숙함
- Airflow 3.x에서 DAG 버전 관리, Asset 기반 이벤트 스케줄링 등이 보완됨

### 6.4 하이브리드 패턴 (실무에서 많이 씀)

```
[Airflow (MWAA)]   ── 데이터 파이프라인: 수집 → 정제 → 학습 데이터셋 생성 (S3)
        │  트리거 (Argo REST API / KFP SDK / S3 이벤트)
        ▼
[Argo / KFP on EKS] ── ML 파이프라인: 학습(GPU) → 평가 → MLflow 등록 → 배포
```

### 6.5 선택 기준

| 상황 | 추천 |
|---|---|
| EKS 중심, 플랫폼/DevOps 팀이 운영, GitOps 중시 | **Argo Workflows (+ Hera)** |
| 데이터 사이언티스트가 Python으로 작성, 실험 비교·계보 추적 중요 | **Kubeflow Pipelines** (standalone 설치) |
| 이미 Airflow를 쓰거나 ETL 비중이 큼 | **Airflow (MWAA) + KubernetesPodOperator**, 또는 하이브리드 |

**이번 AI Factory 기본안**: Argo Workflows + Hera. 실험 관리 요구가 커지면 KFP를 추가합니다.

---

## 7. 주의사항 요약

- **GPU 확보**: p5/p6는 리전별로 물량이 부족함. Capacity Blocks나 ODCR로 예약하고 Karpenter `capacityReservationSelectorTerms`로 연결
- **EFA와 AZ**: 멀티노드 학습은 같은 AZ, 같은 Placement Group에 배치
- **이미지 크기**: CUDA/PyTorch 이미지는 10GB 이상이므로 SOCI나 EBS 스냅샷 프리캐시 사용 (FSR 필수)
- **모델 로딩**: S3에서 직접 순차 다운로드하지 말고 병렬 스트리밍이나 캐시 계층 사용
- **비용**: 학습 클러스터는 작업 후 자동 축소, 추론은 Savings Plans와 Spot 활용, 체크포인트는 Lifecycle으로 정리
- **보안**: Pod Identity로 최소 권한, KMS, VPC Endpoints, 운영 모델 버킷에는 Object Lock

---

## 8. 다음 단계 (미결정 사항)

- [ ] 리전 결정 (예: `us-east-1`, `ap-northeast-2`)
- [ ] 우선순위 결정: 학습 중심 vs 추론 중심
- [ ] 기존 Airflow 사용 여부 확인 (하이브리드 구성 여부)
- [ ] IaC 도구 확정 (Terraform 권장)
- [ ] 저장소 골격 생성: `terraform/`, `gitops/`, `nodepools/`, `examples/`
