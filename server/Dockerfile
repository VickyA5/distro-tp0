FROM python:3.9.7-slim
COPY server /
RUN rm -f /config.ini 2>/dev/null || true
RUN python -m unittest tests/test_common.py
ENTRYPOINT ["/bin/sh"]
