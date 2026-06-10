# Kronos Trading System — Linux ops
# Usage: make <target>
# Requires supervisor to be installed and configured (run install.sh first).

.PHONY: help start stop restart status logs update install \
        tail-data tail-risk tail-dashboard tail-exec tail-pos \
        tail-m13 tail-m14 tail-m15 tail-m16

GROUP := kronos

help:
	@echo ""
	@echo "  Kronos Trading System — available commands"
	@echo ""
	@echo "  Cluster control:"
	@echo "    make start       start all modules"
	@echo "    make stop        stop all modules"
	@echo "    make restart     restart all modules"
	@echo "    make status      show process status"
	@echo ""
	@echo "  Logs:"
	@echo "    make logs        tail all log files (Ctrl+C to exit)"
	@echo "    make tail-data   tail M01 data collection"
	@echo "    make tail-risk   tail M05 risk check (regime filter)"
	@echo "    make tail-exec   tail M06 execution"
	@echo "    make tail-pos    tail M07 position monitor"
	@echo "    make tail-m13    tail M13 mini-1H generator"
	@echo "    make tail-m14    tail M14 base-1H generator"
	@echo "    make tail-m15    tail M15 mini-4H generator"
	@echo "    make tail-m16    tail M16 base-4H generator (benchmark)"
	@echo "    make tail-dashboard  tail M12 dashboard"
	@echo ""
	@echo "  Maintenance:"
	@echo "    make update      git pull + restart all"
	@echo "    make install     (re)run install.sh"
	@echo ""

start:
	sudo supervisorctl start $(GROUP):*

stop:
	sudo supervisorctl stop $(GROUP):*

restart:
	sudo supervisorctl restart $(GROUP):*

status:
	sudo supervisorctl status

logs:
	tail -f logs/*.log

update:
	git pull --ff-only
	sudo supervisorctl restart $(GROUP):*
	@echo "Updated and restarted."

install:
	bash install.sh

# ── Per-module log tails ──────────────────────────────────────────────────────

tail-data:
	tail -f logs/m01_data_collection.log

tail-risk:
	tail -f logs/m05_risk_check.log

tail-exec:
	tail -f logs/m06_execution.log

tail-pos:
	tail -f logs/m07_position_monitor.log

tail-m13:
	tail -f logs/m13_mini_generator.log

tail-m14:
	tail -f logs/m14_base_generator.log

tail-m15:
	tail -f logs/m15_mini_4h_generator.log

tail-m16:
	tail -f logs/m16_base_4h_generator.log

tail-dashboard:
	tail -f logs/m12_dashboard.log
