# Build stage
FROM public.ecr.aws/lambda/python:3.12 AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

# The build context is 'lambda/'
WORKDIR /build/lambda
COPY cmr_poller ./cmr_poller

WORKDIR /build/lambda/cmr_poller
RUN uv pip install --python /var/lang/bin/python3.12 --target /var/task --no-cache .

# Runtime stage
FROM public.ecr.aws/lambda/python:3.12

COPY --from=builder /var/task /var/task

CMD ["handler.handler"]
