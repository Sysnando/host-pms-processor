# Padrão de Conector Climber – Instruções para Novos Projetos

Este documento descreve o **fluxo padrão** e a **estrutura** que todo conector PMS (Property Management System) da Climber deve seguir. Use-o no **novo projeto** como referência única para implementar ou revisar a orquestração (download, S3, ESB, fila), mantendo a lógica de transformação já desenvolvida.

---

## 1. Visão geral do fluxo

O conector deve executar **sempre nesta ordem**:

| # | Etapa | Descrição |
|---|--------|-----------|
| 1 | **Código do hotel** | Usar `HOTEL_CODE` (API do PMS) e `HOTEL_CODE_S3` (caminhos S3/ESB/fila) |
| 2 | **Download dos dados** | Buscar dados no PMS (API, arquivo, etc.) usando o código do hotel |
| 3 | **Raw no S3** | Salvar resposta bruta no bucket de raw (ex.: `qa-pms-raw-reservations`) |
| 4 | **Transformação** | Aplicar a lógica de transformação (já existente no projeto) |
| 5 | **Reservas no S3** | Salvar reservas transformadas no bucket de reservations |
| 6 | **Segmentos no S3** | Salvar segmentos transformados no bucket de segments |
| 7 | **Cadastro no ESB** | Registrar arquivo de reservas e de segmentos no ESB |
| 8 | **Fila (SQS)** | Enviar mensagem na fila do processador (trigger) |

Qualquer conector novo deve seguir exatamente essa sequência; a única parte que muda entre projetos é a **fonte dos dados** (cliente/API) e a **lógica de transformação**.

---

## 2. Estrutura de projeto recomendada

Dois estilos são aceitos; o importante é que **responsabilidades** fiquem separadas.

### Opção A – Estrutura flat (ex.: conector-omnibees)

```
projeto/
├── src/
│   ├── __init__.py
│   ├── main.py           # Orquestração (fluxo 1–8)
│   ├── config.py         # Config centralizada (env)
│   ├── auth.py           # Autenticação ESB (e PMS se necessário)
│   ├── <pms>_api.py      # Cliente de download (ex.: omnibees_api.py)
│   ├── transformers.py   # Transformação reservas + segmentos
│   ├── storage.py        # S3 (raw, reservations, segments)
│   ├── esb_client.py     # Registro de arquivos no ESB
│   ├── sqs_client.py     # Envio de mensagem SQS
│   └── utils.py          # Datas, helpers
├── tests/
├── .env / .env.example
├── requirements.txt
├── run.py                # Ponto de entrada (python run.py)
└── README.md
```

### Opção B – Estrutura por camadas (ex.: HOST-PMS-PROCESSOR)

```
projeto/
├── src/
│   ├── __init__.py
│   ├── main.py
│   ├── config/           # Configurações
│   ├── clients/          # Clientes de API/download do PMS
│   ├── models/           # Modelos de dados
│   ├── services/         # Orquestração e regras de negócio
│   ├── transformers/     # Toda a lógica de transformação
│   └── aws/              # S3, SQS (ou storage + sqs_client no nível acima)
├── tests/
├── .env / .env.example
├── pyproject.toml ou requirements.txt
├── Dockerfile
└── README.md
```

No novo projeto, se já existir `transformers/` com a lógica pronta, basta:

- Manter **transformers** como está.
- Garantir um **service** (ou `main.py`) que execute o fluxo 1–8.
- Ter módulos de **config**, **cliente de download**, **storage (S3)**, **ESB** e **SQS** conforme abaixo.

---

## 3. Configuração obrigatória (Config / .env)

Variáveis que **todo** conector deve ter:

```bash
# Hotel
HOTEL_CODE=...           # Código na API do PMS
HOTEL_CODE_S3=...        # Código nos caminhos S3/ESB/fila (ex.: BRNPEDCO)

# ESB (Climber)
ESB_BASIC_AUTH=...       # Base64
ESB_AUTH_URL=https://qa-esb.climberrms.com:9443/oauth2/token
ESB_RESERVATIONS_URL=https://qa-esb.climberrms.com/pms-integration/1.0/pmsReservation
ESB_SEGMENTS_URL=https://qa-esb.climberrms.com/pms-integration/1.0/pmsSegment

# AWS
AWS_REGION=eu-west-2
# Opcional em desenvolvimento (SSO); obrigatório em VM/Pod (ver seção 3.1)
# AWS_ACCESS_KEY_ID=...
# AWS_SECRET_ACCESS_KEY=...

# S3 Buckets
S3_RAW_RESERVATIONS_BUCKET=qa-pms-raw-reservations
S3_RESERVATIONS_BUCKET=qa-pms-reservations
S3_SEGMENTS_BUCKET=qa-pms-segments

# SQS
SQS_QUEUE_URL=https://sqs.eu-west-2.amazonaws.com/.../qa-pms-processor-queue.fifo
SQS_MESSAGE_GROUP_ID=...  # Geralmente igual ao HOTEL_CODE_S3
```

As específicas do PMS (URLs, credenciais, paginação, período) ficam no mesmo `Config`/`.env` (ex.: `PAGE_SIZE`, `DAYS_BACK`, `DAYS_AHEAD`).

### 3.1 Credenciais AWS (desenvolvimento vs produção)

- **Desenvolvimento (máquina local):** não é obrigatório configurar `AWS_ACCESS_KEY_ID` e `AWS_SECRET_ACCESS_KEY`. O código deve usar as credenciais do ambiente (SSO com `aws sso login`, AWS CLI configurado, etc.). Ou seja: se as variáveis não estiverem definidas, o cliente boto3 usa o provider padrão (perfil, env, instance role).
- **Produção (VM ou Pod Kubernetes):** não há SSO. É necessário garantir acesso à AWS de uma destas formas:
  - **Variáveis de ambiente:** definir `AWS_ACCESS_KEY_ID` e `AWS_SECRET_ACCESS_KEY` no ambiente da VM ou no deployment do Pod (Secret/ConfigMap).
  - **IAM Role:** em VM EC2 usar instance profile; em Kubernetes usar IRSA (IAM Roles for Service Accounts) e não definir access key.
O conector deve ser implementado de forma que: **se** `AWS_ACCESS_KEY_ID` e `AWS_SECRET_ACCESS_KEY` estiverem definidos, use-os; **senão**, use o provider padrão do boto3 (SSO, role, etc.). Assim funciona na máquina do dev e na VM/Pod com credenciais injetadas.

---

## 4. Contratos por etapa (o que implementar)

### 4.1 Código do hotel

- Ler `HOTEL_CODE` e `HOTEL_CODE_S3` de `Config` (ou env).
- Usar `HOTEL_CODE` em todas as chamadas ao PMS.
- Usar `HOTEL_CODE_S3` em: caminhos S3, payloads ESB e corpo/grupo da mensagem SQS.

### 4.2 Download dos dados

- Um único ponto de entrada que recebe período (e opcionalmente hotel_code).
- Retornar o **payload bruto** do PMS (ex.: um `dict` com a resposta da API), **sem** alterar estrutura para “acumular páginas” de forma diferente da resposta original (evitar chaves como `allReservations` no raw).
- Se houver paginação, fazer internamente e devolver no mesmo formato que uma única resposta (ex.: mesma estrutura por página agregada de forma padrão).

### 4.3 Raw no S3

- Bucket: `S3_RAW_RESERVATIONS_BUCKET`.
- Caminho: `{HOTEL_CODE_S3}/reservations-{timestamp}.json`.
- Conteúdo: JSON da resposta bruta do passo “Download” (ex.: um único objeto que representa a resposta da API).
- Timestamp em ISO (ex.: `2024-07-04T11:26:32Z`).

### 4.4 Transformação

- Entrada: mesmo payload bruto que foi para o S3 raw.
- Saídas:
  - Lista de reservas no **formato Climber** (uma lista de objetos por linha/diária).
  - Objeto de **segmentos** (channels, companies, groups, rates, rooms, segments, etc.).
- Manter toda a lógica de transformação em `transformers` ou `transformers.py`; o `main` apenas chama essas funções.

### 4.5 Reservas e segmentos no S3

- **Reservas:** bucket `S3_RESERVATIONS_BUCKET`, path `{HOTEL_CODE_S3}/reservations-{timestamp}.json`, corpo = lista de reservas transformadas (JSON).
- **Segmentos:** bucket `S3_SEGMENTS_BUCKET`, path `{HOTEL_CODE_S3}/segments-{timestamp}.json`, corpo = objeto de segmentos (JSON).
- Usar o **mesmo** `timestamp` em raw, reservations e segments.

### 4.6 Cadastro no ESB

- Autenticação: token OAuth (ex.: Basic Auth no endpoint de token).
- **Reservas:** POST em `ESB_RESERVATIONS_URL` com payload no formato:

  ```json
  {
    "payload": {
      "code": "<HOTEL_CODE_S3>",
      "record_date": "<timestamp até segundos>",
      "last_updated": "<timestamp até segundos>",
      "complete": false,
      "file": "<HOTEL_CODE_S3>/reservations-<timestamp>.json"
    }
  }
  ```

- **Segmentos:** POST em `ESB_SEGMENTS_URL` com o mesmo formato, trocando `file` para `segments-<timestamp>.json`.
- `file` = caminho no S3 **sem** nome do bucket (só key).

### 4.7 Fila (SQS)

- Fila: `SQS_QUEUE_URL` (FIFO).
- Corpo da mensagem: `HOTEL_CODE_S3` (string).
- `MessageGroupId`: `SQS_MESSAGE_GROUP_ID` (em geral igual a `HOTEL_CODE_S3`).

---

## 5. Orquestração no `main` (pseudocódigo)

O `main.py` (ou service principal) deve seguir este esqueleto:

```python
# 1. Validar config (Config.validate())
# 2. Gerar período e timestamp (ex.: generate_dates(DAYS_BACK, DAYS_AHEAD))
# 3. Download: raw_data = client.fetch_...(hotel_code=Config.HOTEL_CODE, ...)
#    - Se vazio, log e return
# 4. Upload raw: storage.upload_raw_reservations(raw_data, timestamp, Config.HOTEL_CODE_S3)
#    - Se falhar, log e return
# 5. Transformar: reservations = transform_reservations(raw_data), segments = transform_segments(...)
#    - Se não houver reservas, log e return
# 6. Upload reservations: storage.upload_reservations(reservations, timestamp, Config.HOTEL_CODE_S3)
# 7. Upload segments: storage.upload_segments(segments, timestamp, Config.HOTEL_CODE_S3)
# 8. ESB: esb.register_reservation_file(...); esb.register_segment_file(...)
# 9. SQS: sqs.send_processor_message(Config.HOTEL_CODE_S3, Config.SQS_MESSAGE_GROUP_ID)
# 10. Log de resumo (arquivos, contagens)
```

Tratamento de erro: em falha crítica (upload, ESB, SQS), logar e encerrar (return/exit); não seguir para as etapas seguintes.

---

## 6. Checklist para o novo projeto

- [ ] **Config**: `HOTEL_CODE`, `HOTEL_CODE_S3`, ESB, AWS, S3 buckets, SQS (env + `Config.validate()` se necessário).
- [ ] **Cliente de download**: recebe hotel + período; retorna payload bruto no formato da API.
- [ ] **Storage (S3)**: `upload_raw_reservations`, `upload_reservations`, `upload_segments` com paths `{HOTEL_CODE_S3}/reservations-{ts}.json` e `{HOTEL_CODE_S3}/segments-{ts}.json`.
- [ ] **Transformers**: entram com raw, saem lista de reservas + objeto de segmentos (manter lógica já existente).
- [ ] **ESB**: auth (token), `register_reservation_file` e `register_segment_file` com payload acima.
- [ ] **SQS**: envio com `MessageBody=HOTEL_CODE_S3`, `MessageGroupId=SQS_MESSAGE_GROUP_ID`.
- [ ] **main**: fluxo 1–8 na ordem, mesmo timestamp em todos os arquivos, logs por etapa.
- [ ] **.env.example** com todas as variáveis (valores de exemplo ou placeholder).
- [ ] **README**: como rodar (`run.py` ou `python -m src.main`), variáveis principais, estrutura do projeto.

---

## 7. Usar Skills/Regras do Cursor para manter o padrão

Para que o Cursor siga sempre este padrão em projetos de conector:

### 7.1 Regra no próprio repositório (recomendado)

1. Crie a pasta `.cursor/rules/` no **novo projeto** (ou no conector-omnibees se quiser referência em todos).
2. Crie um arquivo `.mdc`, por exemplo `conector-climber-padrao.mdc`:

```markdown
---
description: Padrão de conector PMS Climber - fluxo S3, ESB, SQS
alwaysApply: true
---

# Conector Climber

Este projeto é um conector PMS. Siga sempre o documento **CONECTOR_PADRAO_CLIMBER.md** na raiz do repositório.

- Fluxo: código do hotel → download → raw S3 → transformação → reservations/segments S3 → registro ESB → mensagem SQS.
- Código do hotel: HOTEL_CODE (API) e HOTEL_CODE_S3 (S3/ESB/fila).
- Raw: resposta bruta da API, sem alterar estrutura; path {HOTEL_CODE_S3}/reservations-{timestamp}.json no bucket raw.
- Reservas e segmentos: mesmo timestamp; registrar ambos no ESB; depois enviar HOTEL_CODE_S3 na fila SQS.
- Em dúvida, priorize o que está em CONECTOR_PADRAO_CLIMBER.md.
```

Assim, em qualquer conversa nesse repositório, o Cursor tende a seguir o fluxo e os contratos descritos no markdown.

### 7.2 Skill global (opcional)

Se você usar **Cursor Skills** em um diretório compartilhado (ex.: `~/.cursor/skills/` ou no repositório de skills da equipe):

- Crie uma skill que diga: “Em repositórios que contenham `CONECTOR_PADRAO_CLIMBER.md`, tratar o projeto como conector Climber e aplicar o fluxo e a estrutura descritos nesse arquivo.”
- A skill pode incluir um resumo do fluxo (1–8) e apontar para o markdown como referência completa.

Assim, ao abrir qualquer conector que tenha esse markdown, o mesmo padrão é aplicado.

### 7.3 O que colocar no novo projeto

- Copie **CONECTOR_PADRAO_CLIMBER.md** para a raiz do novo projeto.
- Crie **.cursor/rules/conector-climber-padrao.mdc** com o conteúdo da seção 7.1 (e ajuste o nome do arquivo de referência se mudar).
- Ao desenvolver ou pedir ajuda ao Cursor, mencione: “Seguir o CONECTOR_PADRAO_CLIMBER.md”.

Isso mantém o mesmo padrão de projeto em todos os conectores e ajuda tanto humanos quanto IA a seguirem as mesmas instruções.

---

## 8. Referência rápida – arquivos por responsabilidade

| Responsabilidade | Exemplo (flat) | Exemplo (camadas) |
|------------------|----------------|-------------------|
| Orquestração | `src/main.py` | `src/main.py` ou `src/services/orchestrator.py` |
| Config | `src/config.py` | `src/config/` |
| Download PMS | `src/omnibees_api.py` | `src/clients/` |
| Transformação | `src/transformers.py` | `src/transformers/` |
| S3 | `src/storage.py` | `src/aws/` ou `src/storage.py` |
| ESB | `src/esb_client.py` | `src/clients/esb_client.py` ou `src/aws/` |
| SQS | `src/sqs_client.py` | `src/aws/` |
| Auth | `src/auth.py` | `src/auth.py` ou em `config`/`clients` |
| Utils (datas, etc.) | `src/utils.py` | `src/utils.py` ou em `services` |

---

**Resumo:** Coloque este markdown no novo projeto e, se quiser, a regra em `.cursor/rules/`. Implemente o fluxo 1–8 na orquestração, mantendo a lógica de transformação já existente e respeitando os contratos de S3, ESB e SQS acima. Assim o conector fica alinhado ao padrão Climber e fácil de manter.

---

## 9. Prompt para iniciar (Cursor / IA)

Ao abrir o **novo projeto** de conector no Cursor, use um destes prompts para o assistente seguir o padrão desde o início:

**Prompt curto:**

```
Este projeto é um conector PMS Climber. Siga o CONECTOR_PADRAO_CLIMBER.md na raiz: implemente a orquestração completa (código do hotel, download dos dados, raw no S3, transformação já existente, upload de reservations e segments, registro no ESB, envio na fila SQS). Use o mesmo timestamp em todos os arquivos. Credenciais AWS: se AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY estiverem definidas, use-as; senão use o provider padrão do boto3 (SSO em dev, role em prod).
```

**Prompt mais detalhado (se a estrutura já existir):**

```
Preciso integrar este conector ao padrão Climber. Já temos a lógica de transformação em src/transformers (ou transformers/). Preciso que você:

1. Leia o CONECTOR_PADRAO_CLIMBER.md na raiz.
2. Implemente ou ajuste a orquestração para o fluxo: hotel code → download dos dados (usando o cliente existente) → upload raw no S3 → chamar os transformers existentes → upload de reservations e segments no S3 → registrar os dois arquivos no ESB → enviar mensagem na fila SQS.
3. Config e clientes AWS (S3/SQS): usar credenciais explícitas (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY) apenas se estiverem definidas; caso contrário deixar o boto3 usar o ambiente (SSO na máquina, credenciais ou IAM role na VM/Pod).
4. Manter o mesmo timestamp em raw, reservations e segments; paths no formato {HOTEL_CODE_S3}/reservations-{timestamp}.json e {HOTEL_CODE_S3}/segments-{timestamp}.json.

Comece pela checklist do CONECTOR_PADRAO_CLIMBER.md e implemente o que faltar.
```

Com a regra em `.cursor/rules/` ativa, muitas vezes basta dizer: **"Siga o CONECTOR_PADRAO_CLIMBER.md e implemente o fluxo completo do conector; AWS com suporte a SSO em dev e credenciais em produção."**
