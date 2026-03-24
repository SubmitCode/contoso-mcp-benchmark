import os
import json
from dataclasses import dataclass
from typing import Callable

import openai
import anthropic
import google.generativeai as genai


@dataclass
class RunResult:
    model: str
    prompt_id: str
    server: str
    input_tokens: int
    output_tokens: int
    tool_calls: int
    final_answer: str
    error: str = None


def _tools_to_openai(tools: list[dict]) -> list[dict]:
    return [
        {
            "type": "function",
            "function": {
                "name": t["name"],
                "description": t["description"],
                "parameters": t.get("inputSchema", {"type": "object", "properties": {}}),
            },
        }
        for t in tools
    ]


def run_openai(prompt: str, tools: list[dict], call_tool: Callable, model: str = "gpt-4o") -> RunResult:
    client = openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    messages = [{"role": "user", "content": prompt}]
    oai_tools = _tools_to_openai(tools)
    total_input, total_output, tool_call_count = 0, 0, 0

    for _ in range(10):  # max 10 turns
        resp = client.chat.completions.create(model=model, messages=messages, tools=oai_tools)
        total_input += resp.usage.prompt_tokens
        total_output += resp.usage.completion_tokens
        choice = resp.choices[0]

        if choice.finish_reason == "stop":
            return RunResult(
                model=model, prompt_id="", server="",
                input_tokens=total_input, output_tokens=total_output,
                tool_calls=tool_call_count, final_answer=choice.message.content,
            )

        if choice.finish_reason == "tool_calls":
            messages.append(choice.message)
            for tc in choice.message.tool_calls:
                tool_call_count += 1
                result = call_tool(tc.function.name, json.loads(tc.function.arguments))
                messages.append({"role": "tool", "tool_call_id": tc.id, "content": json.dumps(result)})

    return RunResult(model=model, prompt_id="", server="", input_tokens=total_input,
                     output_tokens=total_output, tool_calls=tool_call_count,
                     final_answer="", error="Max turns exceeded")


def run_anthropic(prompt: str, tools: list[dict], call_tool: Callable, model: str = "claude-sonnet-4-6") -> RunResult:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    messages = [{"role": "user", "content": prompt}]
    ant_tools = [
        {
            "name": t["name"],
            "description": t["description"],
            "input_schema": t.get("inputSchema", {"type": "object", "properties": {}}),
        }
        for t in tools
    ]
    total_input, total_output, tool_call_count = 0, 0, 0

    for _ in range(10):
        resp = client.messages.create(model=model, max_tokens=4096, messages=messages, tools=ant_tools)
        total_input += resp.usage.input_tokens
        total_output += resp.usage.output_tokens

        if resp.stop_reason == "end_turn":
            answer = next((b.text for b in resp.content if hasattr(b, "text")), "")
            return RunResult(model=model, prompt_id="", server="",
                             input_tokens=total_input, output_tokens=total_output,
                             tool_calls=tool_call_count, final_answer=answer)

        if resp.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": resp.content})
            tool_results = []
            for block in resp.content:
                if block.type == "tool_use":
                    tool_call_count += 1
                    result = call_tool(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result),
                    })
            messages.append({"role": "user", "content": tool_results})

    return RunResult(model=model, prompt_id="", server="", input_tokens=total_input,
                     output_tokens=total_output, tool_calls=tool_call_count,
                     final_answer="", error="Max turns exceeded")


def run_gemini(prompt: str, tools: list[dict], call_tool: Callable, model: str = "gemini-1.5-pro") -> RunResult:
    genai.configure(api_key=os.environ["GOOGLE_API_KEY"])

    def _make_function(t: dict):
        return genai.protos.FunctionDeclaration(
            name=t["name"],
            description=t["description"],
            parameters=genai.protos.Schema(type=genai.protos.Type.OBJECT),
        )

    gemini_tools = genai.protos.Tool(function_declarations=[_make_function(t) for t in tools])
    client = genai.GenerativeModel(model, tools=[gemini_tools])
    chat = client.start_chat()
    total_input, total_output, tool_call_count = 0, 0, 0

    resp = chat.send_message(prompt)
    for _ in range(10):
        # Capture tokens for every response including the first
        if resp.usage_metadata:
            total_input += resp.usage_metadata.prompt_token_count or 0
            total_output += resp.usage_metadata.candidates_token_count or 0

        part = resp.candidates[0].content.parts[0]

        # Return immediately if this is a final text answer (may happen on first turn)
        if hasattr(part, "text") and part.text:
            return RunResult(model=model, prompt_id="", server="",
                             input_tokens=total_input, output_tokens=total_output,
                             tool_calls=tool_call_count, final_answer=part.text)

        if hasattr(part, "function_call"):
            tool_call_count += 1
            fc = part.function_call
            result = call_tool(fc.name, dict(fc.args))
            resp = chat.send_message(
                genai.protos.Content(parts=[genai.protos.Part(
                    function_response=genai.protos.FunctionResponse(
                        name=fc.name, response={"result": json.dumps(result)}
                    )
                )])
            )
        else:
            break

    return RunResult(model=model, prompt_id="", server="", input_tokens=total_input,
                     output_tokens=total_output, tool_calls=tool_call_count,
                     final_answer="", error="Max turns exceeded")
