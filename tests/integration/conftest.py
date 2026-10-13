import os


def pytest_terminal_summary(terminalreporter):
    if not os.getenv("DEV_STACK"):
        terminalreporter.write_line(
            "Integration tests skipped: they need Postgres and the mock Teamleader API "
            "running in Docker. Run ./.dev/run.sh to start them and run these tests.",
            yellow=True,
        )
