# Desireinfoweb Marketing Voice Agent (Twilio + ElevenLabs)

This project is now tailored for a marketing voice flow:

- Start outbound call by entering a mobile number
- Agent gives a short 2-line company overview
- Agent asks: business automation or IT services
- Agent resolves prospect queries
- Call is limited to 2 minutes (`MAX_CALL_SECONDS=120`)
- If prospect is interested, transfer to human agent (Sajid)
- All call + webhook conversation events are stored in one file (`data/conversation-history.jsonl`)

## 1) Prerequisites

- Node.js 18+
- Twilio account + voice-enabled phone number
- ElevenLabs Conversational AI agent
- ngrok (optional, for public callback URLs during local testing)

## 2) Local setup

```bash
npm install
npm start
```

Open local tester:

- `http://localhost:3000/tester`

Health endpoint:

- `http://localhost:3000/health`

## 3) Environment variables

Copy `.env.example` and fill values.

Important variables:

- `ELEVENLABS_API_KEY`
- `ELEVENLABS_AGENT_ID`
- `TWILIO_ACCOUNT_SID`
- `TWILIO_AUTH_TOKEN`
- `TWILIO_PHONE_NUMBER`
- `COMPANY_NAME=Desireinfoweb`
- `HUMAN_AGENT_NAME=Sajid`
- `HUMAN_AGENT_PHONE=+1xxxxxxxxxx`
- `HUMAN_AGENT_BACKUP_PHONE=+1xxxxxxxxxx` (optional, rings in parallel with Sajid)
- `MAX_CALL_SECONDS=120`
- `HUMAN_TRANSFER_TIMEOUT_SECONDS=45`
- `PUBLIC_BASE_URL=https://<your-ngrok-or-host-domain>`
- `TRANSFER_DUPLICATE_WINDOW_SECONDS=25`
- `CONVERSATION_LOG_FILE=data/conversation-history.jsonl`
- `TRANSFER_RESOLVE_MAX_ATTEMPTS=12`
- `TRANSFER_RESOLVE_RETRY_MS=1000`
- `TRANSFER_AUTO_TRIGGER_ENABLED=true`
- `TRANSFER_AUTO_TRIGGER_PHRASES=connect you to sajid,connecting you to sajid,transfer you to sajid`
- `MIN_AI_CONVERSATION_SECONDS_BEFORE_TRANSFER=20` (prevents premature handoff before AI has spoken with caller)
- `ENABLE_PARALLEL_BACKUP_DIAL=false` (set `true` only if you want Sajid + backup to ring simultaneously)
- `ELEVENLABS_WEBHOOK_REQUIRE_SIGNATURE=false`
- `ELEVENLABS_WEBHOOK_SECRET=optional_if_signature_enabled`

## 4) ngrok setup (optional)

1. Run app locally on port 3000.
2. Start tunnel: `ngrok http 3000`
3. Copy HTTPS URL and set:
4. `PUBLIC_BASE_URL` in `.env`

## 5) ElevenLabs configuration (critical)

In your ElevenLabs agent:

1. Set first message + system prompt to follow this flow:
2. Intro in 2 lines about Desireinfoweb
3. Ask: "Are you looking for business automation or IT services?"
4. Answer user queries briefly
5. Ask: "Would you like to know more about this service?"
6. If user says yes, call transfer tool immediately

Suggested system prompt for smoother behavior:

```text
You are Desireinfoweb voice assistant.

Flow rules:
1) Start with exactly 2 short intro lines about Desireinfoweb.
2) Ask: "Are you looking for business automation or IT services?"
3) Answer briefly and ask one clarifying question at a time.
4) If user asks for more details, consultation, human support, or says "connect me", call transfer tool immediately.
5) Do not delay transfer once user confirms interest.
```

You can also fetch this as JSON from:

- `GET /tester/agent-playbook`

Recommended transfer tool:

- Tool URL: `https://<your-domain>/voice/transfer-to-human`
- Method: `POST`
- Body example: `{ "callerPhone": "+27700000001", "callSid": "CAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "reason": "Prospect asked to connect" }`
- Optional call SID fields accepted: `callSid`, `CallSid`, `call_sid`, `twilioCallSid`, `twilio_call_sid`
- Optional caller fields accepted: `callerPhone`, `caller_phone`, `phone`, `to`, `customerPhone`, `customer_phone`
- Legacy URL `/voice/transfer-to-sajid` still works.

Notes:

- Real in-call transfer requires this tool call during the conversation.
- If transfer rings and drops, increase `HUMAN_TRANSFER_TIMEOUT_SECONDS` and check `GET /tester/recent-events` for `twilio-transfer-leg-*` statuses.
- Duplicate transfer tool calls in a short window are safely ignored to prevent multiple parallel transfer legs.
- Configure ElevenLabs webhook to `POST https://<your-domain>/elevenlabs/webhook` so transcript/events are stored in one file.
- Transfer resolution now retries briefly, so manual browser refresh of `/voice/transfer-to-human` is not required.
- If Sajid sometimes does not ring, check `twilio-transfer-leg-*` events for `sip=487`/`busy`/`failed` (human leg unreachable) and verify carrier availability.
- When the webhook contains an agent message like "connecting you to Sajid", the server now auto-triggers transfer.
- If your tunnel is offline (`ERR_NGROK_3200`), callbacks break and transfer automation fails. Verify with `GET /tester/public-url-health`.
- `POST /tester/start-call` now blocks when `PUBLIC_BASE_URL` health check fails, so callback failures are visible immediately.

## 6) Testing flow from browser

Use `http://localhost:3000/tester`:

1. Enter mobile number and click `Start Real Call`
2. During the live AI conversation, ask to connect to Sajid and confirm transfer happens in-call

## 7) Deploy and host (recommended: Railway)

### Option A: Railway (quickest)

1. Push project to GitHub
2. Create new Railway project from repo
3. Add all environment variables from `.env`
4. Deploy
5. Set Twilio + ElevenLabs URLs to Railway domain

### Option B: Render / Fly.io / VPS

Use any Node.js host that supports:

- Public HTTPS URL
- Environment variables

## 8) Production recommendations

- Add authentication on transfer endpoint so only trusted callers can invoke it

## 9) Endpoints

- `GET /health`
- `GET /tester`
- `GET /tester/config-status`
- `GET /tester/agent-playbook`
- `GET /tester/conversation-log`
- `GET /tester/public-url-health`
- `POST /tester/start-call`
- `POST /voice/transfer-to-sajid`
- `GET /voice/transfer-to-sajid`
- `POST /voice/transfer-to-human`
- `GET /voice/transfer-to-human`
- `POST /elevenlabs/webhook`
- `POST /twilio/transfer-leg-status`
- `GET /tester/recent-events`
