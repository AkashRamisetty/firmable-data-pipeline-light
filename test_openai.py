import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise RuntimeError("OPENAI_API_KEY not found in environment / .env")

from openai import OpenAI

client = OpenAI(api_key=api_key)

resp = client.chat.completions.create(
    model="gpt-4.1-mini",
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Reply with a single short sentence saying hello."},
    ],
    temperature=0,
)

print("LLM reply:", resp.choices[0].message.content.strip())
