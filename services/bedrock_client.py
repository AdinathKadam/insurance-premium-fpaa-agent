import json
import os
from typing import Optional

import boto3
from dotenv import load_dotenv


load_dotenv()


def get_bedrock_client():
    region = os.getenv("AWS_REGION", "us-east-1")

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = os.getenv("AWS_SESSION_TOKEN")

    client_kwargs = {
        "service_name": "bedrock-runtime",
        "region_name": region,
    }

    # If keys are present, use them.
    # If not, boto3 will fall back to the normal AWS credential chain.
    if access_key and secret_key:
        client_kwargs["aws_access_key_id"] = access_key
        client_kwargs["aws_secret_access_key"] = secret_key
        if session_token:
            client_kwargs["aws_session_token"] = session_token

    return boto3.client(**client_kwargs)


def _invoke_anthropic_model(
    prompt: str,
    model_id: str,
    max_tokens: int = 700,
    temperature: float = 0.2
) -> str:
    client = get_bedrock_client()

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "temperature": temperature,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }
        ]
    }

    response = client.invoke_model(
        modelId=model_id,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(body)
    )

    raw = response["body"].read()
    parsed = json.loads(raw)

    content = parsed.get("content", [])
    text_chunks = []

    for item in content:
        if item.get("type") == "text":
            text_chunks.append(item.get("text", ""))

    final_text = "\n".join([x for x in text_chunks if x]).strip()

    if not final_text:
        raise ValueError("Bedrock returned an empty response.")

    return final_text


def generate_bedrock_text(
    prompt: str,
    max_tokens: int = 700,
    temperature: float = 0.2,
    model_id: Optional[str] = None
) -> str:
    resolved_model_id = model_id or os.getenv("BEDROCK_MODEL_ID")

    if not resolved_model_id:
        raise ValueError(
            "BEDROCK_MODEL_ID is not set. Add it in .env or pass model_id explicitly."
        )

    # This code path is designed for Anthropic-style Bedrock models.
    if "anthropic" not in resolved_model_id.lower():
        raise ValueError(
            "This implementation expects an Anthropic Bedrock model. "
            "Set BEDROCK_MODEL_ID to an approved Anthropic model ID."
        )

    return _invoke_anthropic_model(
        prompt=prompt,
        model_id=resolved_model_id,
        max_tokens=max_tokens,
        temperature=temperature
    )