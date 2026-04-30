# Flash Sale Orchestrator

> Event processing platform for high-concurrency flash sales. Architecture complete and tested: a few final pieces before production.

**Status:** 🚀 Ongoing Development

---

## 📹 Quick Overview

**Watch the 3-minute project overview:** [Video Link - Coming Soon]

Learn what this project solves and why it matters in just 3 minutes.

---

## 🛠️ Tech Stack

- **Language:** Go
- **Event Streaming:** Apache Kafka / Redpanda
- **Database:** PostgreSQL
- **Caching:** Redis
- **Payment Processing:** Stripe
- **Stress Testing:** k6
- **Observability:** Prometheus + Grafana
- **Containerization:** Docker

---

## 🎯 The Problem

Imagine you're building a website that sells concert tickets. Tickets are limited. The sale opens at a fixed time. The moment it does — **thousands of users hit the site at once trying to buy those limited tickets.**

Your job as a developer is to ensure:
- ✅ The system stays responsive — the website and database don't get overwhelmed by concurrent requests
- ✅ **No overselling(consequence of race conditions) or underselling** occurs
- ✅ Users have a **good experience**
- ✅ The system stays resilient **even when services fail temporarily**
- ✅ **Rollback mechanisms** exist when payments fail but reservations were made
- ✅ Idempotency — sending the same request twice has the same result as sending it once
- ✅ No partial writes — avoid scenarios where a reservation is saved to Redis but the event fails to publish to Kafka (the outbox worker ensures atomic writes)


The key insight: *Anyone can build a system that works when everything goes right. The skill is building one that survives when things go wrong.*

---

## 🏗️ Architecture Approach

A simple monolith or 3-tier architecture won't cut it for this use case. This project implements a **sophisticated event-driven architecture** with multiple specialized workers handling different concerns:

- **Reservation Worker** — Handles ticket reservations while preventing race conditions
- **Payment Worker** — Processes payments via Stripe webhooks
- **Rollback Worker** — Handles failed payments and cleans up reservations
- **Outbox Worker** — Ensures reliable event publishing
- **Sold-Out Worker** — Updates user status when tickets sell out
- **Reservation Persistence Worker** — Reads reservation and payment events from Kafka, syncing state to the database

Each worker is independently scalable and can handle partial failures without compromising system correctness.

---

## 📚 Detailed Video Series (Coming Soon)

This is just the beginning. I'm creating an **in-depth video series** exploring every aspect of this project:

### Video Playlist: [Detailed Deep-Dives - Coming Soon]

#### 1. **Architecture Decisions & Tradeoffs** 
Dive deep into the architectural choices I considered, why I chose what I chose, and the tradeoffs between different approaches. 

#### 2. **Anticipating Failure Scenarios**
Failure is inevitable. In this video, I walk through every failure scenario I identified (service crashes, network partitions, payment failures, etc.) and show exactly how the system detects and recovers from each one. 

#### 3. **Integration, Stress & Chaos Testing**
Claiming a system is resilient isn't enough—you need to prove it. This video covers:
- **Integration Testing** — Verifying happy paths work (successful reservations, successful payments)
- **Edge Case Testing** — Ensuring bad input doesn't crash the system and Testing idempotency (duplicate requests don't cause duplicate effects)
- **Stress Testing** — Simulating many concurrent users to prove the system holds up under heavy load
- **Chaos Testing** — Randomly killing services to verify the system recovers gracefully

#### 4. **Limitations & Lessons Learned**
The mistakes I made, limitations of my current approach, and how I would do things differently next time.

---

## 🚀 What's Next (Future Work)

### 1. **Distributed Tracing**
Debugging a distributed system with multiple services is one of the biggest challenges I faced. Before working further, I will be implementing distributed tracing across all workers to get end-to-end visibility into request flows.

### 2. **Scale Testing on EC2**
Currently, stress testing runs on the same machine as the system, causing OOM crashes at ~1000 concurrent users (the k6 load generator consumes more memory than the system itself). The next step is to deploy on separate EC2 instances—one running the application, another running k6—so I can reach higher concurrency and get more realistic results.

### 3. **Naive Implementation Comparison**
To truly validate that my approach is better, I'm planning to implement a quick, naive 3-tier architecture for the same use case and compare:
- Throughput
- Latency
- Resource consumption
- Error rates under load

Then I'll publish side-by-side metrics to show the quantifiable difference between approaches.

With these three pieces complete, the system will be ready for production.
---

## 🎓 Key Takeaways

**Lesson #1: Architecture is about tradeoffs**
There's no perfect architecture. Every approach has pros and cons. Your job is to understand your use case and choose tradeoffs that matter most to you.

**Lesson #2: The happy path doesn't matter**
The happy path is the least important. It's the edge cases and failure scenarios that break your business. Build systems that survive when things go wrong, not just when they go right.

**Lesson #3: Tests are your safety net**
Writing tests alongside your code feels like extra work, until you need to refactor or add a new feature. Suddenly, instead of manually checking whether your changes broke something, you just run your test suite. Green means you're good. Red tells you exactly what broke and where. The time you invest writing tests early pays back every single time you touch the codebase later.

---

## 📝 License

See LICENSE file

---

**Questions? Feedback?** This is an ongoing project. More detailed content coming soon!

---
