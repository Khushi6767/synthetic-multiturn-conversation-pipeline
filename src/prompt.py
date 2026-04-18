SYSTEM_PROMPT = (
    "You are a helpful assistant. "
    "Always wrap your reasoning inside <think> </think> tags "
    "and your final answer inside <answer> </answer> tags."
)

PROMPT_TEMPLATE = """Convert the following Q&A into a natural multi-turn conversation with between 2 and 8 messages (always an even number, alternating user then assistant, starting with user).

The user is curious and engaged. Their follow-up questions must:
- React naturally to what the assistant just said
- Be based on BOTH the original question AND the assistant's previous response
- Show they understood the answer and are building on it
- Feel like a real person talking

Every assistant message MUST follow this exact format:
<think>step by step reasoning here</think> <answer>complete sentence answer here</answer>

STRICT RULES:
- Between 2 and 8 messages total (even number only)
- Only <think> and <answer> tags
- Complete sentences in every <answer>
- No code fences, no preamble
- Start with [ and end with ]

Question: {question}
Answer: {answer}

Return ONLY the JSON array."""
