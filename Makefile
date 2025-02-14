.PHONY: install-dev-tools
install-dev-tools:
	cargo build --release
	sudo install -m755 target/release/check_schema /usr/bin/
	sudo install -m755 target/release/diff_value /usr/bin/
