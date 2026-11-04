# Take-Home Exercise: Build a Data Pipeline

**Time:** 2–3 hours. **Submit:** code + your output table + a short `NOTES.md`.

We're less interested in a "finished" product than in **how you think** about building a pipeline. A smaller, well-reasoned solution beats a large one with hidden bugs. Your written reasoning counts as much as your code.

---

## The scenario

A retail company's order system drops a **CSV file of orders into storage once per day**. A separate **product reference file** describes each product. Analysts want a daily sales table they can use for reporting.

You're given three files (see `data_dictionary.md` for column details):

- `orders_2024-01-01.csv` — day 1 drop
- `orders_2024-01-02.csv` — day 2 drop
- `products.csv` — product reference

---

## What to build

A pipeline that ingests these raw files and produces a single analytics-ready table answering the business question below. **How you structure the pipeline between the raw input and the final table is entirely up to you** — that design is part of what we're evaluating.

**Business question — the output table.** Produce a table that lets analysts see, **per day, per product category, per region**:

- net revenue
- order count
- units sold
- average order value (AOV)

---

## Ground rules

- **Design for a daily schedule, not a one-time load.** Assume `orders_2024-01-03.csv`, `-04`, etc. will keep arriving with the same shape, and that the pipeline runs each day as a new file lands.
- **The role works on Databricks (Delta Lake), but you do *not* need Databricks access for this exercise.** Build it on **PostgreSQL** (or local PySpark if you prefer). Structure it the way you'd build a production pipeline; where you'd do something differently on Databricks/Delta (e.g. `MERGE`, Auto Loader, constraints), just say so in your notes. We're evaluating your design thinking and engineering judgement — not platform-specific syntax.
- Use whatever language/tools you're comfortable with: SQL, Python/pandas, PySpark — your choice.
- It's fine to make assumptions where the requirements leave room — just **state them**.

---

## What to submit

1. **Code** — your pipeline (SQL and/or Python), runnable or clearly explained.
2. **The output table** — the actual output rows, or a query that produces them.
3. **`NOTES.md`** — a short writeup covering:
   - Any assumptions you made and any decisions where the problem left room for judgement (and why you chose as you did).
   - Anything about the data that influenced how you built the pipeline.
   - What you'd do differently with more time, or to take this to production / much larger volumes.
   - Where your local approach would differ from what you'd build on Databricks/Delta.

---

## How we'll evaluate it

We score **reasoning over completeness**. Specifically:

- **Design & structure** — how you organize the pipeline: clear, maintainable, and appropriate for a job that runs repeatedly in production.
- **Robustness & correctness** — the pipeline holds up to what the real data contains, and the output numbers are right given your stated rules.
- **Engineering judgement** — sensible, well-reasoned decisions where the problem is open-ended, and awareness of the trade-offs.
- **Communication** — clear assumptions and trade-offs in your notes.

There will be a follow-up conversation where we'll ask *why* you made certain choices and *how* you'd adapt if the requirements or the data changed. Build with that conversation in mind.

Good luck — and remember, done-and-reasoned beats perfect-but-silent.
