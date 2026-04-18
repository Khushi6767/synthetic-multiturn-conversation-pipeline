# Synthetic Multi-Turn Conversation Generation Pipeline

## Problem
Most publicly available datasets are single-turn (one question → one answer).  
However, real-world interactions are multi-turn and require reasoning across multiple steps.

This mismatch limits the ability of models to handle follow-up questions and maintain context.

---

## Objective
To convert single-turn Q&A datasets into structured multi-turn conversations with explicit reasoning traces at scale.

---

## Approach

We built a scalable and extensible pipeline that:

- Uses LLM (GPT-OSS-120B) to generate multi-turn conversations  
- Simulates natural, context-aware follow-up questions  
- Adds structured reasoning using:
  - `<think>` → step-by-step reasoning  
  - `<answer>` → final response  
- Ensures strict JSON format compliance  
- Retries failed outputs  
- Removes duplicate samples using hashing  
- Supports parallel execution for large-scale generation  

---

## Pipeline

1. Load dataset from HuggingFace  
2. Generate multi-turn conversation using LLM  
3. Parse and validate JSON output  
4. Retry failed generations  
5. Deduplicate using hashing  
6. Store output in parquet format  

---

## Prompt Design

We designed a high-quality prompt enforcing:

- Natural conversational flow  
- Context-aware follow-up questions  
- Strict output schema  
- Explicit reasoning tags  

The prompt was iteratively refined to fix:

- Missing `<think>` tags  
- Invalid JSON outputs  
- Inconsistent formatting  

---

## Model Selection

**Final Model:** GPT-OSS-120B  

**Reasons:**
- Best format compliance  
- Stable outputs  
- Consistent reasoning tags  
- Minimal truncation errors  

---

## Results (Ongoing)

- Total samples processed so far: **400K+**  
- Successfully converted: **300K+**  
- Current success rate: **~70–80%**  

⚡ The pipeline is actively running to generate more data across multiple datasets.

---

## Key Insight
Most failures are due to formatting issues (JSON/tags), not reasoning quality.

---

## Installation

```bash
pip install -r requirements.txt
```

---

## How to Run

Run the pipeline:

```bash
python src/pipeline.py --dataset <dataset_name> --n 10
```

### Example

```bash
python src/pipeline.py --dataset openai/gsm8k --n 5
```

### Arguments
- `--dataset` → HuggingFace dataset name  
- `--n` → number of samples to process  

### Note
- Set your API key as an environment variable: `GPTOSS_API_KEY`  
- The pipeline automatically handles retries and parsing  

---

## Running on Cluster (Background Execution)

```bash
nohup python src/pipeline.py --dataset <dataset_name> &
```

This allows the process to continue even after logging out.

---

## Project Structure

```
synthetic-multiturn-conversation-pipeline/
│
├── src/
│   ├── pipeline.py   # main pipeline
│   ├── prompt.py     # prompt design
│   ├── parser.py     # parsing logic
│
├── docs/
│   └── project_report.md
│
├── results/
│   └── metrics.md
│
├── requirements.txt
├── .gitignore
├── .env.example
├── README.md
```

---

## Security

API keys are not stored in the repository.  
They are accessed via environment variables.

---

## Future Work

- Improve parsing robustness  
- Add semantic quality filtering  
- Evaluate dataset via fine-tuning  
- Extend to more datasets and domains  

---

## Authors

Khushi Garg  
Ankit Saha  
IIT Gandhinagar
