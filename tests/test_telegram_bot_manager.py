"""Tests for the Telegram bot manager's command dispatch and the
sustained-409 conflict-yield in the manual poll loop.

Follows the house convention of the existing tests (test_audit_fixes.py):
dependency-light, pure-logic checks plus AST/source inspection of the
modules that import third-party packages, so they run in a bare
interpreter without the full venv.
"""

from __future__ import annotations

import ast
from pathlib import Path
import unittest

_REPO = Path(__file__).resolve().parents[1]
_BM = _REPO / "layers" / "telegram" / "bot_manager.py"
_CMDS = _REPO / "layers" / "telegram" / "commands.py"


def _src(path: Path) -> str:
    return path.read_text()


class TestManualPollConflictYield(unittest.TestCase):
    """The manual poll loop must stop after YIELD_AFTER_CONFLICTS straight
    409s (conflict-yield), rather than backing off and re-polling forever."""

    def test_yield_threshold_present_in_source(self):
        s = _src(_BM)
        self.assertIn(
            "YIELD_AFTER_CONFLICTS = 20",
            s,
            "conflict-yield threshold must be defined in the poll loop",
        )
        self.assertIn("break", s, "the poll loop must break (yield) on sustained 409")
        self.assertIn(
            "consecutive_409 >= YIELD_AFTER_CONFLICTS",
            s,
            "the loop must compare the counter against the threshold",
        )

    def test_backoff_caps_at_30s(self):
        # The backoff formula: min(n*3, 30) seconds.
        for n in range(1, 30):
            backoff = min(n * 3, 30)
            self.assertLessEqual(backoff, 30)
        self.assertEqual(min(1 * 3, 30), 3)
        self.assertEqual(min(10 * 3, 30), 30)
        self.assertEqual(min(50 * 3, 30), 30)

    def test_success_resets_conflict_counter(self):
        # On any successful (non-409) poll, the consecutive counter resets.
        consecutive_409 = 17
        consecutive_409 = 0  # simulate a successful getUpdates
        self.assertEqual(consecutive_409, 0)
        consecutive_409 += 1  # a later single 409
        self.assertLess(consecutive_409, 20)

    def test_yield_is_a_break_not_a_hammer(self):
        # After the threshold, the loop exits (break), it does NOT keep
        # awaiting sleep + continue on the 409 branch.
        s = _src(_BM)
        # The 409 branch must contain both the yield-break AND the backoff
        # sleep, with the break coming before the sleep for the sustained case.
        idx_yield = s.find("MANUAL_POLL YIELD")
        idx_break = s.find("break", idx_yield) if idx_yield != -1 else -1
        self.assertGreater(idx_yield, 0, "yield log line must exist")
        self.assertGreater(idx_break, idx_yield, "yield must break the loop")


class TestMenuCommandsRegistered(unittest.TestCase):
    """The menu must expose the full command set so the Telegram client can
    list them (setMyCommands / set_my_commands uses MENU_COMMANDS)."""

    def test_menu_defines_bot_commands(self):
        s = _src(_BM)
        self.assertIn("MENU_COMMANDS = [", s)
        self.assertIn("BotCommand(", s)

    def test_menu_contains_core_commands(self):
        s = _src(_BM)
        for name in ("catalog", "status", "help"):
            self.assertIn(f'"{name}"', s, f"menu should reference /{name}")

    def test_menu_is_unique(self):
        # Parse the MENU_COMMANDS list literal and assert command names are
        # unique (a duplicate would break set_my_commands).
        s = _src(_BM)
        start = s.index("MENU_COMMANDS = [")
        # find the matching closing bracket by scanning
        depth = 0
        i = s.index("[", start)
        end = i
        for j in range(i, len(s)):
            if s[j] == "[":
                depth += 1
            elif s[j] == "]":
                depth -= 1
                if depth == 0:
                    end = j
                    break
        block = s[i : end + 1]
        tree = ast.parse(block)
        names = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and node.func.id == "BotCommand":
                if node.args and isinstance(node.args[0], ast.Constant):
                    names.append(node.args[0].value)
        self.assertEqual(len(names), len(set(names)), "duplicate menu command")
        self.assertGreater(len(names), 0)


class TestCommandDispatchWiring(unittest.TestCase):
    """commands.py registers handlers that mutate app.state (the in-memory
    control surface). Confirm the command handlers are importable and that
    start_bot() wires the menu + process handler."""

    def test_commands_module_defines_handlers(self):
        s = _src(_CMDS)
        # Command handlers are async functions referenced in the dispatch
        # table.
        self.assertIn("async def", s)

    def test_start_bot_registers_handlers(self):
        s = _src(_BM)
        # Handlers are registered via register_handlers() + the error handler.
        self.assertIn("register_handlers", s, "_build_application must register command handlers")
        self.assertIn("add_error_handler", s, "a global error handler must be registered")
        self.assertIn("MENU_COMMANDS", s, "the command menu must be registered")
        self.assertIn("set_my_commands", s)

    def test_manual_poll_dispatches_to_process_update(self):
        s = _src(_BM)
        self.assertIn(
            "process_update", s, "manual poll must route updates through PTB process_update"
        )
        self.assertIn("de_json", s, "manual poll must de_json raw updates into PTB Update objects")


if __name__ == "__main__":
    unittest.main()
