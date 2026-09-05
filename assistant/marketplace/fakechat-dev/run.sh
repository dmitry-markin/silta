#!/bin/bash
# Runs the official fakechat server (already installed in the plugin cache) on a fixed port.
export FAKECHAT_PORT=8793
exec bun run --cwd /home/bob/.claude/plugins/cache/claude-plugins-official/fakechat/0.0.1 --silent start
