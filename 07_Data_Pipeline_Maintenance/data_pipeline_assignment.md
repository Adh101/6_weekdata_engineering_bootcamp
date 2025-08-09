# Data Pipeline Maintenance Assignment

## Business Context
We are four engineers (**A**, **B**, **C**, **D**) who work at **Streamflix**, a global video streaming platform.

- Customers subscribe monthly or annually.
- They can purchase add-ons like live sports, ad-free plans, or extra simultaneous streams.
- Core metrics are used for both **investor reporting** and **internal product experiments**.

---

## Pipelines Overview

1. **Profit**
   - **Definition:** Subscription revenue − content licensing costs − operational costs.
   - **Unit-level profit:** Profit per subscriber (+ cost per subscriber).

2. **Growth**
   - **Definition:** Increase in total subscribers, plus upsells to higher-tier plans.

3. **Engagement**
   - **Definition:** Viewer activity: active days, hours watched, completion rates.

4. **Aggregate Pipeline to Executives/CFO**
   - Weekly investor-facing rollups from Profit, Growth, Engagement.

5. **Aggregate Pipeline to Experiment Team**
   - High-granularity data for A/B testing new features (recommendation algorithms, ad formats, playback controls).

---

## Severity Levels

| Severity | Description | Typical Impact | Response Expectation |
|----------|-------------|----------------|----------------------|
| **Sev-1 (Critical)** | Pipeline is down, data is late, wrong, or missing for investor-facing reports. | CFO, executives, or investors are getting incomplete/incorrect data. Could impact financial reporting or public disclosures. | **24/7 coverage** — Primary Owner paged immediately, must acknowledge within 15 minutes and work until resolved or mitigated. |
| **Sev-2 (High)** | Pipeline is delayed or has partial data for internal experiment/analysis pipelines, but doesn’t block investor reports. | Experiment teams may have to postpone tests or make decisions with incomplete data, but no regulatory/investor risk. | **Business hours coverage** — Acknowledge within 1 hour, aim to resolve the same day. |
| **Sev-3 (Medium)** | Minor data quality issues or small metric discrepancies that do not block decisions. | May require backlog fixes or quality improvement. | Fix during normal sprint cycles; no immediate escalation. |

---

## Runbooks

### 1️⃣ Unit-Level Profit Pipeline (Experiments)

**Purpose:**  
Calculate per-subscriber profitability for experimental pricing, new feature bundles, or promotional offers.

**Ownership:**
- Primary Owner: **A**
- Secondary Owner: **C**

**Schedule:**
- Runs daily at **T+4h** from midnight CT.

**Severity & On-Call:**
- Severity: **Sev-2** (affects experiment teams only).
- On-Call Rotation: Primary covers business hours; Secondary covers holidays or if Primary unavailable.
- Escalation: Respond within 1 hour during business hours; fix same day.

**Upstream Dependencies:**
- Subscription billing system
- Content licensing cost feed
- Ad revenue data
- Experiment assignment logs

**Checks:**
- Primary key uniqueness: `(subscriber_id, billing_period)`
- Plan cost matches catalog table
- No negative profit unless refund issued

**Steps to Run:**
1. Validate billing & cost partitions are complete.
2. Join costs to each subscriber’s billing record.
3. Allocate content costs per subscriber.
4. Calculate profit per subscriber.
5. Tag each subscriber with experiment group.
6. Save to experiment schema.

**Risks:**
- Late ad revenue feed
- Content cost allocation mismatch
- Missing or corrupted experiment assignment tags

---

### 2️⃣ Aggregate Profit Pipeline (Investors)

**Purpose:**  
Compute total platform profitability for executive and investor reports.

**Ownership:**
- Primary Owner: **A**
- Secondary Owner: **B**

**Schedule:**
- Runs weekly and monthly (investor snapshot).

**Severity & On-Call:**
- Severity: **Sev-1** (critical for investor reporting).
- On-Call Rotation: 24/7 coverage; Secondary covers holiday day.
- Escalation: Acknowledge within 15 min; escalate to Secondary after 15 min, to Eng Manager after 30 min.

**Upstream Dependencies:**
- Subscription billing
- Ad revenue
- Content & infrastructure costs
- FX rates
- Finance GL system

**Checks:**
- Revenue matches payment processor ±0.5%
- Content cost coverage ≥99.5%
- Primary key uniqueness

**Steps to Run:**
1. Load all revenue and cost feeds.
2. Normalize currencies.
3. Aggregate totals per period.
4. Reconcile with finance GL.
5. Publish to investor dashboards.

**Risks:**
- Late content partner cost reports
- Foreign exchange mismatches
- Finance API outages

---

### 3️⃣ Aggregate Growth Pipeline (Investors)

**Purpose:**  
Track subscriber growth trends for investor communication.

**Ownership:**
- Primary Owner: **B**
- Secondary Owner: **D**

**Schedule:**
- Runs weekly.

**Severity & On-Call:**
- Severity: **Sev-1** (critical for investor reporting).
- On-Call Rotation: 24/7 coverage; Secondary covers holiday day.
- Escalation: Respond within 15 min; escalate to Secondary after 15 min.

**Upstream Dependencies:**
- Subscriber sign-up logs
- Plan change history
- Churn event logs

**Checks:**
- Deduplicate subscriber IDs
- Exclude bot/spam accounts
- Verify correct classification of upgrades/downgrades

**Steps to Run:**
1. Validate subscriber data freshness.
2. Count new sign-ups, churns, upgrades, downgrades.
3. Calculate net growth.
4. Publish to investor dashboard.

**Risks:**
- Fraudulent sign-ups inflating numbers
- Delay in churn data ingestion
- Upgrade events misclassified

---

### 4️⃣ Daily Growth Pipeline (Experiments)

**Purpose:**  
Provide daily subscriber trend data for A/B tests and feature rollouts.

**Ownership:**
- Primary Owner: **B**
- Secondary Owner: **A**

**Schedule:**
- Runs daily at **T+2h**.

**Severity & On-Call:**
- Severity: **Sev-2** (important for experiments, but not investor reports).
- On-Call Rotation: Business hours only; Secondary covers holidays.
- Escalation: Respond within 1 hour during business hours.

**Upstream Dependencies:**
- Subscriber events feed
- Experiment assignment logs

**Checks:**
- Experiment coverage ≥97%
- Daily sign-up counts match logs ±2%

**Steps to Run:**
1. Load subscriber event data.
2. Assign each subscriber to experiment group.
3. Calculate daily growth metrics.
4. Store in experiment schema.

**Risks:**
- Delayed experiment logs
- Returning subscribers misclassified as new

---

### 5️⃣ Aggregate Engagement Pipeline (Investors)

**Purpose:**  
Aggregate DAU/WAU/MAU and viewing metrics for investor updates.

**Ownership:**
- Primary Owner: **C**
- Secondary Owner: **A**

**Schedule:**
- Runs weekly.

**Severity & On-Call:**
- Severity: **Sev-1** (critical for investor reporting).
- On-Call Rotation: 24/7 coverage; Secondary covers holidays.
- Escalation: Respond within 15 min; escalate to Secondary after 15 min.

**Upstream Dependencies:**
- Viewing activity logs
- Device metadata
- Feature usage logs

**Checks:**
- Sessionization logic correct
- Bot/spam detection applied
- Event-time skew <10 minutes median

**Steps to Run:**
1. Ingest viewing logs.
2. Apply sessionization and deduplication.
3. Aggregate DAU/WAU/MAU and average hours watched.
4. Publish to investor dashboard.

**Risks:**
- Event schema changes breaking transformations
- Bot activity spikes
- Delay in device metadata refresh
