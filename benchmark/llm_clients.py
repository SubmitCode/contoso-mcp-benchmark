import os
import json
from dataclasses import dataclass
from typing import Callable, Optional

import openai
import anthropic
import google.generativeai as genai

genai.configure(api_key=os.environ.get("GOOGLE_API_KEY", ""))


def _fmt_tool_call(name: str, args: dict) -> str:
    MAX = 80
    parts = []
    for k, v in args.items():
        s = str(v)
        if len(s) > MAX:
            s = s[:MAX] + "..."
        parts.append(f"{k}={s}")
    return f"  → {name}({', '.join(parts)})"


@dataclass
class RunResult:
    model: str
    prompt_id: str
    server: str
    input_tokens: int
    output_tokens: int
    tool_calls: int
    final_answer: str
    error: Optional[str] = None


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


_REASONING_MODELS = {"gpt-5.3-codex"}  # models that use max_completion_tokens


def run_openai(prompt: str, tools: list[dict], call_tool: Callable, model: str = "gpt-5.3-chat-latest") -> RunResult:
    client = openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    messages = [{"role": "user", "content": prompt}]
    oai_tools = _tools_to_openai(tools)
    total_input, total_output, tool_call_count = 0, 0, 0

    for _ in range(10):  # max 10 turns
        # Reasoning/codex models use max_completion_tokens; standard models use max_tokens
        kwargs: dict = {"model": model, "messages": messages, "tools": oai_tools}
        if model.startswith("o") or model in _REASONING_MODELS:
            kwargs["max_completion_tokens"] = 4096
        else:
            kwargs["max_tokens"] = 4096

        resp = client.chat.completions.create(**kwargs)
        total_input += resp.usage.prompt_tokens
        total_output += resp.usage.completion_tokens
        choice = resp.choices[0]

        if choice.finish_reason == "stop":
            return RunResult(
                model=model, prompt_id="", server="",
                input_tokens=total_input, output_tokens=total_output,
                tool_calls=tool_call_count, final_answer=choice.message.content or "",
            )

        elif choice.finish_reason == "tool_calls":
            messages.append(choice.message)
            for tc in choice.message.tool_calls:
                tool_call_count += 1
                args = json.loads(tc.function.arguments) if tc.function.arguments else {}
                print(_fmt_tool_call(tc.function.name, args))
                try:
                    result = call_tool(tc.function.name, args)
                    result_content = json.dumps(result) if not isinstance(result, str) else result
                except Exception as exc:
                    result_content = json.dumps({"error": str(exc)})
                messages.append({"role": "tool", "tool_call_id": tc.id, "content": result_content})

        else:
            return RunResult(model=model, prompt_id="", server="",
                             input_tokens=total_input, output_tokens=total_output,
                             tool_calls=tool_call_count, final_answer="",
                             error=f"Unexpected finish_reason: {choice.finish_reason}")

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

        elif resp.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": resp.content})
            tool_results = []
            for block in resp.content:
                if block.type == "tool_use":
                    tool_call_count += 1
                    print(_fmt_tool_call(block.name, block.input))
                    try:
                        result = call_tool(block.name, block.input)
                        result_content = json.dumps(result) if not isinstance(result, str) else result
                    except Exception as exc:
                        result_content = json.dumps({"error": str(exc)})
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result_content,
                    })
            messages.append({"role": "user", "content": tool_results})

        else:
            return RunResult(model=model, prompt_id="", server="",
                             input_tokens=total_input, output_tokens=total_output,
                             tool_calls=tool_call_count, final_answer="",
                             error=f"Unexpected stop_reason: {resp.stop_reason}")

    return RunResult(model=model, prompt_id="", server="", input_tokens=total_input,
                     output_tokens=total_output, tool_calls=tool_call_count,
                     final_answer="", error="Max turns exceeded")


def run_gemini(prompt: str, tools: list[dict], call_tool: Callable, model: str = "gemini-1.5-pro") -> RunResult:
    def _make_function(t: dict) -> genai.protos.FunctionDeclaration:
        schema = t.get("inputSchema", {})
        return genai.protos.FunctionDeclaration(
            name=t["name"],
            description=t["description"],
            parameters=genai.protos.Schema(
                type=genai.protos.Type.OBJECT,
                properties={
                    k: genai.protos.Schema(type=genai.protos.Type.STRING)
                    for k in schema.get("properties", {}).keys()
                },
                required=schema.get("required", []),
            ),
        )

    gemini_tools = genai.protos.Tool(function_declarations=[_make_function(t) for t in tools])
    client = genai.GenerativeModel(model, tools=[gemini_tools])
    chat = client.start_chat()

    last_prompt_tokens = 0
    total_output = 0
    tool_call_count = 0

    resp = chat.send_message(prompt)
    for _ in range(10):
        if resp.usage_metadata:
            last_prompt_tokens = resp.usage_metadata.prompt_token_count or 0
            total_output += resp.usage_metadata.candidates_token_count or 0

        if not resp.candidates or not resp.candidates[0].content.parts:
            return RunResult(model=model, prompt_id="", server="",
                             input_tokens=last_prompt_tokens, output_tokens=total_output,
                             tool_calls=tool_call_count, final_answer="",
                             error="Empty response from Gemini")

        text_answer = None
        function_calls = []
        for part in resp.candidates[0].content.parts:
            if hasattr(part, "text") and part.text:
                text_answer = part.text
            if hasattr(part, "function_call") and part.function_call.name:
                function_calls.append(part.function_call)

        if text_answer is not None and not function_calls:
            return RunResult(model=model, prompt_id="", server="",
                             input_tokens=last_prompt_tokens, output_tokens=total_output,
                             tool_calls=tool_call_count, final_answer=text_answer)

        if function_calls:
            response_parts = []
            for fc in function_calls:
                tool_call_count += 1
                print(_fmt_tool_call(fc.name, dict(fc.args)))
                try:
                    result = call_tool(fc.name, dict(fc.args))
                    result_content = json.dumps(result) if not isinstance(result, str) else result
                except Exception as exc:
                    result_content = json.dumps({"error": str(exc)})
                response_parts.append(genai.protos.Part(
                    function_response=genai.protos.FunctionResponse(
                        name=fc.name, response={"result": result_content}
                    )
                ))
            resp = chat.send_message(genai.protos.Content(parts=response_parts))
        else:
            return RunResult(model=model, prompt_id="", server="",
                             input_tokens=last_prompt_tokens, output_tokens=total_output,
                             tool_calls=tool_call_count, final_answer="",
                             error="Empty response from Gemini")

    return RunResult(model=model, prompt_id="", server="",
                     input_tokens=last_prompt_tokens, output_tokens=total_output,
                     tool_calls=tool_call_count, final_answer="", error="Max turns exceeded")
