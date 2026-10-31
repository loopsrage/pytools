"""Command-line interface for agent model slotting / hot-swappable harness.

Usage:
    python applications/agent_brain/cli.py list
    python applications/agent_brain/cli.py load <model_id>
    python applications/agent_brain/cli.py active
    python applications/agent_brain/cli.py run <action> [--input 0.5,0.5,...]
    python applications/agent_brain/cli.py unload
    python applications/agent_brain/cli.py interactive
"""

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
# Ensure the repo root is importable so `applications.agent_brain.*` resolves
# when this file is executed directly as a script.
_REPO_ROOT = str(_HERE.parents[2])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import torch

from applications.agent_brain.models.model_registry import set_models_dir
from applications.agent_brain.models.model_harness import ThreadSafeModelHarness, register_architecture
from applications.agent_brain.models.architectures import *  # noqa: F401  (registers default architectures)


def _cmd_list(harness):
    models = harness.list_models()
    if not models:
        print("No models found.")
        return
    print(f"{'Model ID':<30} {'Format':<12} {'Architecture':<20} {'Description'}")
    print("-" * 80)
    for m in models:
        print(
            f"{m['model_id']:<30} {m['format']:<12} "
            f"{m['architecture']:<20} {m.get('description', '')}"
        )
    print(f"\nTotal: {harness.model_count()} model(s)")
    agents = harness.list_agents()
    if agents:
        print(f"Active agent slots: {', '.join(agents)}")


def _cmd_load(harness, agent_id, model_id):
    try:
        loaded = harness.load_model(agent_id, model_id)
        print(f"Loaded model: {loaded['model_id']}")
        print(f"  Format:    {loaded['format']}")
        print(f"  Arch:      {loaded['architecture']}")
        print(f"  Input dim: {loaded.get('input_dim')}")
        print(f"  Output dim:{loaded.get('output_dim')}")
        print(f"  Agent:     {agent_id}")
    except Exception as exc:
        print(f"Error loading model '{model_id}' for agent '{agent_id}': {exc}")
        sys.exit(1)


def _cmd_active(harness, agent_id):
    active = harness.active_model(agent_id)
    if active is None:
        print(f"No model currently loaded for agent '{agent_id}'.")
        return
    print(f"Active model for agent '{agent_id}': {active['model_id']}")
    print(f"  Format:    {active['format']}")
    print(f"  Arch:      {active['architecture']}")
    print(f"  Input dim: {active.get('input_dim')}")
    print(f"  Output dim:{active.get('output_dim')}")


def _cmd_unload(harness, agent_id):
    unloaded = harness.unload_model(agent_id)
    if unloaded is None:
        print(f"No model was loaded for agent '{agent_id}'.")
    else:
        print(f"Unloaded model: {unloaded['model_id']} (agent: {agent_id})")


def _cmd_run(harness, agent_id, model_id, action, input_str):
    if model_id:
        try:
            harness.load_model(agent_id, model_id)
        except Exception as exc:
            print(f"Error loading model '{model_id}' for agent '{agent_id}': {exc}")
            sys.exit(1)
    if input_str is None:
        input_data = None
    else:
        try:
            input_data = [float(x.strip()) for x in input_str.split(",")]
        except ValueError:
            print(f"Invalid --input value: {input_str}")
            sys.exit(1)
    result = harness.run_action(agent_id, action, input_data)
    if "error" in result:
        print(f"Error: {result['error']}")
        sys.exit(1)
    if action in ("forward", "predict"):
        print(f"Agent:   {result.get('agent_id', agent_id)}")
        print(f"Model:   {result['active']}")
        print(f"Input:   shape={result['input_shape']}")
        print(f"Output:  {result['output']}")
    elif action == "info":
        print(f"Agent:   {result.get('agent_id', agent_id)}")
        print(f"Active:  {result['active']['model_id']}")
        print(f"Tensors:")
        for t in result.get("tensors", []):
            print(f"  {t['name']}: shape={t['shape']} dtype={t['dtype']}")
    elif action == "tensors":
        print(f"Agent:   {result.get('agent_id', agent_id)}")
        for t in result.get("tensors", []):
            print(f"  {t['name']}: shape={t['shape']} dtype={t['dtype']}")


def _cmd_interactive(harness):
    print("=== Agent Model Harness (interactive) ===")
    print("Commands: list, load <model_id>, active, run <action> [--input v1,v2,...], unload, agent <id>, quit")
    print("Default agent: 'default'")
    print()
    current_agent = "default"
    while True:
        try:
            line = input(f"agent-brain[{current_agent}]> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not line:
            continue
        parts = line.split()
        cmd = parts[0].lower()
        args = parts[1:]

        if cmd in ("quit", "exit", "q"):
            break
        elif cmd == "list":
            _cmd_list(harness)
        elif cmd == "active":
            _cmd_active(harness, current_agent)
        elif cmd == "unload":
            _cmd_unload(harness, current_agent)
        elif cmd == "agent":
            if args:
                current_agent = args[0]
                print(f"Switched to agent: {current_agent}")
            else:
                print(f"Current agent: {current_agent}")
        elif cmd == "load":
            if not args:
                print("Usage: load <model_id>")
                continue
            _cmd_load(harness, current_agent, args[0])
        elif cmd == "run":
            if not args:
                print("Usage: run <action> [--input v1,v2,...]")
                continue
            action = args[0]
            input_str = None
            if "--input" in args:
                i = args.index("--input")
                if i + 1 < len(args):
                    input_str = args[i + 1]
            _cmd_run(harness, current_agent, None, action, input_str)
        else:
            print(f"Unknown command: {cmd}")
    print("Goodbye.")


def main():
    argv = sys.argv[1:]
    if not argv:
        print("Usage: cli.py <command> [args]")
        print("Commands: list, load <agent_id> <model_id>, active <agent_id>, run <agent_id> <model_id> <action> [--input ...], unload <agent_id>, interactive")
        sys.exit(1)

    set_models_dir(_HERE.parent / "models" / "store")
    harness = ThreadSafeModelHarness()

    # Parse optional --agent-id / -a flag
    agent_id = "default"
    if "--agent-id" in argv:
        i = argv.index("--agent-id")
        if i + 1 < len(argv):
            agent_id = argv[i + 1]
            argv = argv[:i] + argv[i + 2:]
    elif "-a" in argv:
        i = argv.index("-a")
        if i + 1 < len(argv):
            agent_id = argv[i + 1]
            argv = argv[:i] + argv[i + 2:]

    cmd = argv[0].lower() if argv else ""
    if cmd == "list":
        _cmd_list(harness)
    elif cmd == "load":
        if len(argv) < 2:
            print("Usage: cli.py load [--agent-id <id>] <model_id>")
            sys.exit(1)
        _cmd_load(harness, agent_id, argv[1])
    elif cmd == "active":
        _cmd_active(harness, agent_id)
    elif cmd == "unload":
        _cmd_unload(harness, agent_id)
    elif cmd == "run":
        if len(argv) < 2:
            print("Usage: cli.py run [--agent-id <id>] <model_id> <action> [--input v1,v2,...]")
            print("       (omits model_id to use currently loaded model for agent)")
            sys.exit(1)
        # First arg may be a model_id or an action
        possible_model = argv[1]
        if len(argv) >= 3 and argv[2] in ("forward", "predict", "info", "tensors"):
            model_id = possible_model
            action = argv[2]
        else:
            model_id = None
            action = possible_model
        input_str = None
        if "--input" in argv:
            i = argv.index("--input")
            if i + 1 < len(argv):
                input_str = argv[i + 1]
        _cmd_run(harness, agent_id, model_id, action, input_str)
    elif cmd == "interactive":
        _cmd_interactive(harness)
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
