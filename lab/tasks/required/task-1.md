# Build the Data Pipeline

<h4>Time</h4>

~50 min

<h4>Purpose</h4>

Build an ETL pipeline that fetches data from an external API and loads it into the database, handling pagination, incremental sync, and idempotent upserts.

<h4>Context</h4>

The database starts empty. The autochecker dashboard API provides anonymized check results for all students across all labs. Your job is to build a pipeline that fetches this data and populates the local database so the system can serve it through existing endpoints and power analytics in the next task.

The code stubs in [`backend/app/etl.py`](../../../backend/app/etl.py) contain detailed TODOs. You will use an AI coding agent to implement the pipeline functions.

<h4>Table of contents</h4>

- [1. Steps](#1-steps)
  - [1.1. Follow the `Git workflow`](#11-follow-the-git-workflow)
  - [1.2. Create a `Lab Task` issue](#12-create-a-lab-task-issue)
  - [1.3. Part A: Explore the API](#13-part-a-explore-the-api)
    - [1.3.1. Fetch the item catalog](#131-fetch-the-item-catalog)
    - [1.3.2. Fetch check logs](#132-fetch-check-logs)
    - [1.3.3. Test incremental sync](#133-test-incremental-sync)
  - [1.4. Part B: Build the pipeline](#14-part-b-build-the-pipeline)
    - [1.4.1. Read the code stubs](#141-read-the-code-stubs)
    - [1.4.2. Implement the pipeline](#142-implement-the-pipeline)
    - [1.4.3. Deploy and test](#143-deploy-and-test)
    - [1.4.4. Verify the data](#144-verify-the-data)
    - [1.4.5. Test idempotency](#145-test-idempotency)
    - [1.4.6. Commit your work](#146-commit-your-work)
  - [1.5. Finish the task](#15-finish-the-task)
- [2. Acceptance criteria](#2-acceptance-criteria)

## 1. Steps

### 1.1. Create an issue

Create a `Lab Task` issue titled: `[Task] Build the Data Pipeline`

> [!TODO:]
>
> they should also create a branch.
>
> may be check issue body
>
> Break line at the end of all curl commands json

> We follow the usual [`Git workflow`](../../../wiki/git-workflow.md) to complete all tasks.

### 1.2. Part A: Explore the API

<!-- no toc -->
- [1.3.1. Fetch the item catalog](#131-fetch-the-item-catalog)
- [1.3.2. Fetch check logs](#132-fetch-check-logs)
- [1.3.3. Test incremental sync](#133-test-incremental-sync)

> [!NOTE]
> Before writing any code, lets explore the autochecker API with `curl` to understand the data format.
> The API uses HTTP Basic Auth — your <email> as the username and `<your-github-username><your-telegram-alias>` as the password.

#### 1.2.1. Fetch the item catalog

1. To fetch the lab/task catalog,

   run in the `VS Code Terminal`:

   ```terminal
   curl -u <your-email>@innopolis.university:<your-github-username><your-telegram-alias> https://auche.namaz.live/api/items
   ```

   Replace `<your-email>` and `<your-github-username><your-telegram-alias>` (must be same as you entered in autochecker bot).

   You should see a JSON array of lab and task objects:

   ```json
   [
     {"lab": "lab-01", "task": null, "title": "Lab 01 – ...", "type": "lab"},
     {"lab": "lab-01", "task": "setup", "title": "Repository Setup", "type": "task"},
     ...
   ]
   ```

   > [!NOTE]
   > Items with `"type": "lab"` are labs. Items with `"type": "task"` have a non-null `task` field and belong to the lab specified in the `lab` field.
   > 
   > You can paste the response in an [online JSON viewer](https://jsonformatter.org/) and press beautify to view it properly.
#### 1.2.2. Fetch check logs

1. To fetch the first 5 check logs,

   run in the `VS Code Terminal`:

   ```terminal
   curl -u <your-email>@innopolis.university:<your-github-username><your-telegram-alias> "https://auche.namaz.live/api/logs?limit=5"
   ```

   You should see a JSON object with a `logs` array:

   ```json
   {
     "logs": [
       {
         "id": 1,
         "student_id": "a1b2c3d4",
         "group": "B23-CS-01",
         "lab": "lab-01",
         "task": "setup",
         "score": 100.0,
         "passed": 4,
         "failed": 0,
         "total": 4,
         "checks": [...],
         "submitted_at": "2026-02-01T14:30:00Z"
       }
     ],
     "count": 5,
     "has_more": true
   }
   ```

   > [!NOTE]
   > - `student_id` is an anonymized identifier (not a real student ID).
   > - `has_more: true` means there are more records — you need to paginate.
   > - `score` is a percentage (0.0–100.0).
   > - `passed`, `failed`, and `total` are the number of individual checks.

#### 1.3.3. Test incremental sync

1. To fetch only recent logs,

   run in the `VS Code Terminal`:

   ```terminal
   curl -u <your-email>@innopolis.university:<your-github-username><your-telegram-alias> "https://auche.namaz.live/api/logs?since=2026-03-01T00:00:00Z&limit=5"
   ```

   You should see only logs submitted after March 1st 2026.

   > [!NOTE]
   > The `since` parameter enables incremental sync — you can fetch new data each time.
   > Your pipeline will use the most recent `submitted_at` from the database as the `since` value.

### 1.4. Part B: Build the pipeline

<!-- no toc -->
- [1.4.1. Read the code stubs](#141-read-the-code-stubs)
- [1.4.2. Implement the pipeline](#142-implement-the-pipeline)
- [1.4.3. Deploy and test](#143-deploy-and-test)
- [1.4.4. Verify the data](#144-verify-the-data)
- [1.4.5. Test idempotency](#145-test-idempotency)
- [1.4.6. Commit your work](#146-commit-your-work)

#### 1.4.1. Read the code stubs

1. Open the file:
   [`backend/app/etl.py`](../../../backend/app/etl.py).

   This file contains five functions with detailed TODO comments:

   | Function | Role |
   |----------|------|
   | `fetch_items()` | Fetch the lab/task catalog from the API |
   | `fetch_logs()` | Fetch check logs with pagination |
   | `load_items()` | Insert items into the database |
   | `load_logs()` | Insert logs (with learner creation) into the database |
   | `sync()` | Orchestrate the full pipeline |

2. Open the file:
   [`backend/app/routers/pipeline.py`](../../../backend/app/routers/pipeline.py).

   This file provides the `POST /pipeline/sync` endpoint that calls `sync()`.

3. Read the TODO comments in `etl.py` carefully. They specify:

   - Which API endpoints to call and how to authenticate.
   - How to handle pagination (`has_more` flag).
   - How to match API data to database models.
   - How to ensure idempotent upserts (skip records that already exist).

#### 1.4.2. Implement the pipeline

1. Start the `Qwen code` coding agent in the terminal inside the project directory.
2. Give it a prompt like:

   > "Read the TODO comments in `backend/app/etl.py` and implement all five functions. Use the existing models in `backend/app/models/` and the settings in `backend/app/settings.py`. The API uses HTTP Basic Auth. Explain to me step by step to maximize learning."

3. Wait for the agent to generate the implementation.

4. Review the generated code. Make sure it:

   - Uses `httpx.AsyncClient` with HTTP Basic Auth for API calls.
   - Handles pagination in `fetch_logs()` (loops while `has_more` is True).
   - Creates learners by `external_id` in `load_logs()` (find-or-create pattern).
   - Uses `external_id` on `InteractionLog` for idempotent upserts (skip if exists).
   - Returns `{"new_records": N, "total_records": M}` from `sync()`.

> [!TIP]
> If you prefer to implement the functions manually, the TODO comments in `etl.py` describe each function step by step.

#### 1.4.3. Deploy and test

1. Push your changes and deploy to the VM.

   On your VM:

   ```terminal
   cd se-toolkit-lab-5
   git fetch origin && git checkout <task-branch> && git pull
   docker compose --env-file .env.docker.secret up --build -d
   ```

   Replace [`<task-branch>`](../../../wiki/git-workflow.md#task-branch).

2. Open [`Swagger UI`](../../../wiki/swagger.md#what-is-swagger-ui) at `http://<your-vm-ip-address>:<caddy-port>/docs`.

   Replace:

   - [`<your-vm-ip-address>`](../../../wiki/vm.md#your-vm-ip-address)
   - [`<caddy-port>`](../../../wiki/caddy.md#caddy-port)

3. [Authorize](../../../wiki/swagger.md#authorize-in-swagger-ui) with your [`API_KEY`](../../../wiki/dotenv-docker-secret.md#api_key).

4. Trigger the pipeline: expand `POST /pipeline/sync`, click `Try it out`, then `Execute`.

   You should see a `200` response with a JSON body:

   ```json
   {
     "new_records": 150,
     "total_records": 150
   }
   ```

   The exact numbers depend on how many check results exist in the autochecker.

   <details><summary>Troubleshooting</summary>

   <h4>401 Unauthorized from the autochecker API</h4>

   Check that `AUTOCHECKER_EMAIL` and `AUTOCHECKER_PASSWORD` are set correctly in `.env.docker.secret`. The password is `<your-github-username><your-telegram-alias>` (no spaces, no `@`).

   <h4>500 Internal Server Error</h4>

   Check the container logs for the error:

   ```terminal
   docker compose --env-file .env.docker.secret logs app --tail 50
   ```

   Common issues: missing import, wrong field name, database constraint violation.

   <h4>Connection refused to the autochecker API</h4>

   Verify that `AUTOCHECKER_API_URL` is set to `https://auche.namaz.live` in `.env.docker.secret`.

   </details>

#### 1.4.4. Verify the data

1. In `Swagger UI`, try `GET /items/`.

   You should see a list of lab and task items created by the pipeline.

2. Try `GET /learners/`.

   You should see a list of learners with anonymized `external_id` values and student groups.

3. Try `GET /interactions/`.

   You should see interaction records with `score`, `checks_passed`, and `checks_total` fields.

4. (Optional) Open [`pgAdmin`](../../../wiki/pgadmin.md#what-is-pgadmin) and inspect the tables directly.

#### 1.4.5. Test idempotency

1. In `Swagger UI`, run `POST /pipeline/sync` again.

   You should see:

   ```json
   {
     "new_records": 0,
     "total_records": 150
   }
   ```

   `new_records: 0` confirms that the pipeline does not create duplicate records.

> [!NOTE]
> Idempotent upserts are important for production pipelines.
> If the pipeline is interrupted, you can safely re-run it without creating duplicates.

#### 1.4.6. Commit your work

1. [Commit](../../../wiki/git-workflow.md#commit) your changes.

   Use this commit message:

   ```text
   feat: implement ETL pipeline for autochecker data
   ```

### 1.5. Finish the task

1. [Create a PR](../../../wiki/git-workflow.md#create-a-pr-to-the-main-branch-in-your-fork) with your changes.
2. [Get a PR review](../../../wiki/git-workflow.md#get-a-pr-review) and complete the subsequent steps in the `Git workflow`.

---

## 2. Acceptance criteria

- [ ] Issue has the correct title.
- [ ] `POST /pipeline/sync` returns `200` with a JSON body containing `new_records` and `total_records`.
- [ ] `GET /items/` returns items created by the pipeline (labs and tasks).
- [ ] `GET /learners/` returns learners created by the pipeline.
- [ ] `GET /interactions/` returns interactions with scores.
- [ ] Running `POST /pipeline/sync` a second time returns `new_records: 0` (idempotency).
- [ ] PR is approved.
- [ ] PR is merged.
