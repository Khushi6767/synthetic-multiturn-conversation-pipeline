"""
FINAL PRODUCTION PIPELINE (HIGH-QUALITY PROMPT + NO DUPLICATES + WORKERS)

- High-quality multi-turn prompt (original version)
- GPT-OSS only
- Parallel workers (default=6)
- Skip duplicates BEFORE API call
- Start offset for chunking
- Append-safe parquet writing
"""
import re
from datasets import load_dataset
import requests, json, pandas as pd, os, argparse, hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── CONFIG ────────────────────────────────────────────────
GPTOSS_URL = "http://172.17.99.11:30000/v1/chat/completions"
GPTOSS_KEY = "AVTXOTWZab9v8WExZMNcGXdCFPCmon4LQPMWP6iS32w2"
GPTOSS_MODEL = "openai/gpt-oss-120b"

OUTPUT_DIR = "generated_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

MADE_BY = "Khushi Garg & Ankit Saha"

# ── HIGH-QUALITY PROMPT (RESTORED FULL VERSION) ───────────
SYSTEM_PROMPT = (
    "You are a helpful assistant. "
    "Always wrap your reasoning inside <think> </think> tags "
    "and your final answer inside <answer> </answer> tags."
)

PROMPT_TEMPLATE = """Convert the following Q&A into a natural multi-turn conversation with between 2 and 8 messages (always an even number, alternating user then assistant, starting with user).

The user is curious and engaged. Their follow-up questions must:
- React naturally to what the assistant just said (not just restate the original question)
- Be based on BOTH the original question AND the assistant's previous response
- Show they understood the answer and are building on it — asking why, what if, how does this generalise, real-world implications, edge cases, or deeper connections
- Feel like a real person talking, not a robotic prompt

Every assistant message MUST follow this exact format:
<think>step by step reasoning here</think> <answer>complete sentence answer here</answer>

STRICT RULES:
- Between 2 and 8 messages total (even number only)
- Only <think> and <answer> tags — no other tags
- Complete sentences in every <answer>
- No code fences, no preamble
- Start with [ and end with ] — nothing before or after

Question: {question}
Answer: {answer}

Return ONLY the JSON array."""

def normalize(conv):
    """Ensure all turns are {role, content} dicts not plain strings."""
    if not conv or not isinstance(conv, list):
        return conv
    normalized = []
    for i, item in enumerate(conv):
        if isinstance(item, str):
            role = "user" if i % 2 == 0 else "assistant"
            normalized.append({"role": role, "content": item})
        elif isinstance(item, dict):
            normalized.append(item)
    return normalized

# ── API CALL ──────────────────────────────────────────────
def call_gptoss(question, answer):
    r = requests.post(
        GPTOSS_URL,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {GPTOSS_KEY}"
        },
        json={
            "model": GPTOSS_MODEL,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",
                 "content": PROMPT_TEMPLATE.format(
                     question=question, answer=answer)},
            ],
            "temperature": 0.7,
            "max_tokens": 2500,
        },
        timeout=120,
    )
    r.raise_for_status()
    data = r.json()
    return data["choices"][0]["message"]["content"], data["usage"]["completion_tokens"]

# ── PARSE OUTPUT ──────────────────────────────────────────
def parse_raw(raw: str):
    if not raw:
        return None
    # Strip common junk
    text = raw.strip().replace("\n", " ")

    # Extract the first complete JSON array (handles leading text)
    match = re.search(r'\[\s*\{.*\}\s*\]', text, re.DOTALL)
    if not match:
        return None

    try:
        obj = json.loads(match.group(0))
        return obj if isinstance(obj, list) else None
    except json.JSONDecodeError:
        return None

# ── WORKER FUNCTION ───────────────────────────────────────
def process_single(i, row, args, existing_ids):
    question = str(row.get(args.question_col, ""))[:2000]
    answer = str(row.get(args.answer_col, ""))[:2000]

    # UNIQUE ID
    unique_id = hashlib.md5((question + answer).encode()).hexdigest()

    # SKIP duplicates BEFORE API call
    if unique_id in existing_ids:
        return "skip"

    for attempt in range(4):  # 0 = first try, 1 = retry
        try:
            raw, tokens = call_gptoss(question, answer)

            # Improved robust parsing
            conv = normalize(parse_raw(raw))

            # Validation
            if (conv and
                isinstance(conv, list) and
                len(conv) >= 2 and
                len(conv) % 2 == 0 and
                all(isinstance(m, dict) and "role" in m and "content" in m for m in conv)):

                return {
                    "id": unique_id,
                    "messages": json.dumps(conv),
                    "num_tokens": tokens,
                    "difficulty_level": "UNKNOWN",
                    "task": args.task,
                    "domain": str(row.get("domain", "")),
                    "language": args.language,
                    "source": args.dataset,
                    "made_by": f"{MADE_BY} | {GPTOSS_MODEL}",
                    "multi_turn": "Yes" if len(conv) > 2 else "No",
                }

            if attempt == 3:   # Last attempt failed
                break   # go to failure return below


        except Exception as e:
            if attempt < 3:
                print(f"[{i}] API error on attempt 1: {type(e).__name__}. Retrying...")
                continue
            else:
                print(f"[{i}] Failed after 4 attempts: {type(e).__name__}")

    # If we reach here → both attempts failed
    return {
        "id": unique_id,
        "messages": None,                    # ← as you wanted
        "num_tokens": 0,
        "difficulty_level": "UNKNOWN",
        "task": args.task,
        "domain": str(row.get("domain", "")),
        "language": args.language,
        "source": args.dataset,
        "made_by": f"{MADE_BY} | {GPTOSS_MODEL}",
        "multi_turn": "No"                   # ← as you wanted
    }

# ── MAIN PIPELINE ─────────────────────────────────────────
def run(args):

    print(f"Loading dataset: {args.dataset}")
    if args.config:
        ds = load_dataset(args.dataset, args.config, split=args.split)
    else:
        ds = load_dataset(args.dataset, split=args.split)

    source_name = args.dataset.replace("/", "_")
    safe_model = GPTOSS_MODEL.replace("/", "_")
    filename = f"{args.domain}_{safe_model}_{source_name}_{args.iteration}.parquet"
    output_path = os.path.join(OUTPUT_DIR, filename)

    # Load existing IDs
    existing_ids = set()
    if os.path.exists(output_path):
        old_df = pd.read_parquet(output_path)
        existing_ids = set(old_df["id"])
        print(f"Existing rows: {len(existing_ids)}")

    start = args.start
    end = min(start + args.n, len(ds))

    print(f"Processing {start} → {end} with {args.workers} workers\n")

    results = []
    skipped = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(process_single, i, ds[i], args, existing_ids)
            for i in range(start, end)
        ]

        for future in as_completed(futures):
            res = future.result()
            if res == "skip":
                skipped += 1
            elif res:
                results.append(res)
            else:
                failed += 1

    print(f"\nNew: {len(results)} | Skipped: {skipped} | Failed: {failed}")

    if not results:
        print("No new data generated.")
        return

    new_df = pd.DataFrame(results)

    # Append + deduplicate (extra safety)
    if os.path.exists(output_path):
        old_df = pd.read_parquet(output_path)
        combined_df = pd.concat([old_df, new_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=["id"])
    else:
        combined_df = new_df

    combined_df.to_parquet(output_path)

    print(f"\nSaved to: {output_path}")
    print(f"Total rows: {len(combined_df)}")

# ── ENTRY POINT ───────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--dataset", required=True)
    parser.add_argument("--question_col", required=True)
    parser.add_argument("--answer_col", required=True)

    parser.add_argument("--n", type=int, default=1000)
    parser.add_argument("--start", type=int, default=0)

    parser.add_argument("--domain", default="general")
    parser.add_argument("--task", default="reasoning")
    parser.add_argument("--language", default="English")
    parser.add_argument("--split", default="train")
    parser.add_argument("--config", default=None)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--iteration", default="1")

    args = parser.parse_args()
    run(args)
