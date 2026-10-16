# Build a separate image with --target spark, api, ui, or browser.

FROM apache/spark:3.5.5-java17-python3@sha256:96dc2bada923824300e1ece564389258934655ba6f463d6daff8753ff0c639ce AS spark
USER root
COPY requirements.txt requirements-dev.txt /tmp/dependencies/
RUN pip install --no-cache-dir -r /tmp/dependencies/requirements-dev.txt
COPY deploy/module4/requirements/api.txt /tmp/module4-api-requirements.txt
RUN pip install --no-cache-dir -r /tmp/module4-api-requirements.txt

# Spark does not bundle the Structured Streaming Kafka source. Keep the
# connector and its compile-time dependencies in the image, pinned and
# checksum-verified so workers never need to resolve Maven packages at runtime.
RUN set -eu; \
    base=https://repo.maven.apache.org/maven2; \
    fetch() { path="$1"; sha="$2"; file="/opt/spark/jars/${path##*/}"; \
      curl -fsSL "$base/$path" -o "$file"; \
      echo "$sha  $file" | sha512sum -c -; \
    }; \
    fetch org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar 613ababeebcfc14239167dd56acaf320bb931db48fef1495301891d71c7cc40d69f7be933f074c5002b83f18555c5aeb4eb81f0c8c8ed33082f814f0f4e01e34; \
    fetch org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar e01ff13050ef5a97b9e98dc1fd75245dd08e9a0ed7ad12a46ac0a5e0203a9e01fcdb12d6fe1640ab26f1d7067ac3884563b16aebb0334b23c9182e86efd7dfc5; \
    fetch org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar b111e1980fd6bf65ca546f6b517fb31a140b19b4dc88646e723b11b54b521e48643f33aa341f47b3111a4556af131200f80c851e1b68d37cb4e747e283eefd7e; \
    fetch org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar 7177d8a9e45ae47881ec323150dc7bd3372ce32eff9866029b47a77cb734535d5e987f22e6cb7009104832c3772079fb6322b2919650765b76a4b4aa8eae648e; \
    curl -fsSL https://repos.spark-packages.org/graphframes/graphframes/0.8.3-spark3.5-s_2.12/graphframes-0.8.3-spark3.5-s_2.12.jar \
      -o /opt/spark/jars/graphframes-0.8.3-spark3.5-s_2.12.jar; \
    echo "0778fb638a84f43de4e9a7d5ebeb8fb489bf0ed0542183e443a1b7a1f67051362095a7dfe98f6aabf16ccb8c77f59d14a09f744bf588a8711964424eb39927e5  /opt/spark/jars/graphframes-0.8.3-spark3.5-s_2.12.jar" | sha512sum -c -
RUN python3 -m pip check
WORKDIR /workspace
ENV PYTHONPATH=/workspace

# Reuse the pinned Spark stage for accepted C5 imports and fixture validation.
FROM spark AS api

USER root
WORKDIR /workspace
COPY deploy/module4/requirements/api.txt /tmp/module4-api-requirements.txt
RUN pip install --no-cache-dir -r /tmp/module4-api-requirements.txt \
    && python3 -m pip check \
    && python3 -c "import pymongo; assert pymongo.version == '4.11.3', pymongo.version"

COPY pyproject.toml /workspace/pyproject.toml
COPY src/ /workspace/src/
COPY tests/ /workspace/tests/
COPY scripts/module4/__init__.py /workspace/scripts/module4/__init__.py
COPY scripts/module4/module4d_api_validation.py /workspace/scripts/module4/module4d_api_validation.py
COPY scripts/module4/module4d_demo_fixture.py /workspace/scripts/module4/module4d_demo_fixture.py
COPY scripts/module4/module4d_demo_validate.py /workspace/scripts/module4/module4d_demo_validate.py
COPY scripts/module4/module4c_responsiveness.py /workspace/scripts/module4/module4c_responsiveness.py
COPY scripts/module4/module4c_streaming_demo.py /workspace/scripts/module4/module4c_streaming_demo.py
COPY docs/evidence/full/analytics.json /workspace/docs/evidence/full/analytics.json
COPY docs/evidence/full/current-hardening-2026-10-03.json /workspace/docs/evidence/full/current-hardening-2026-10-03.json
COPY docs/evidence/module4/full-ab-20261010-fixed-first/evaluation.json /workspace/docs/evidence/module4/full-ab-20261010-fixed-first/evaluation.json
COPY docs/evidence/module4c/c8a-receipt.json /workspace/docs/evidence/module4c/c8a-receipt.json
COPY docs/evidence/module4c/c8b-receipt.json /workspace/docs/evidence/module4c/c8b-receipt.json
ENV PYTHONPATH=/workspace

# The image is exercised with explicit test commands by the validation script.
CMD ["python3", "-m", "pytest", "-q"]

FROM python:3.14-slim@sha256:a2b82f3c48559aa0a8446d9af49826b6e2b2016f4cd2afabfe6013ec53729170 AS ui

ARG MODULE4D_UI_DEV=0
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    HOME=/tmp \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /app
COPY deploy/module4/requirements/ui.txt deploy/module4/requirements/ui-dev.txt ./
RUN python -m pip install --no-cache-dir --disable-pip-version-check \
      -r ui.txt \
    && if [ "$MODULE4D_UI_DEV" = "1" ]; then \
      python -m pip install --no-cache-dir --disable-pip-version-check -r ui-dev.txt; \
    fi \
    && python -m pip check \
    && useradd --uid 10001 --create-home --shell /usr/sbin/nologin dashboard

COPY src/__init__.py ./src/__init__.py
COPY src/module4/__init__.py ./src/module4/__init__.py
COPY src/module4/dashboard/__init__.py ./src/module4/dashboard/__init__.py
COPY src/module4/dashboard/ui ./src/module4/dashboard/ui

USER 10001:10001
EXPOSE 8501
CMD ["python", "-m", "streamlit", "run", "src/module4/dashboard/ui/app.py", "--server.address", "0.0.0.0", "--server.port", "8501"]

FROM mcr.microsoft.com/playwright/python:v1.63.0-noble@sha256:72bd171a9ffc2b4b59532aaa6210e21014d07093120dc25528870c0b840da1f0 AS browser

WORKDIR /opt/module4d-browser
COPY deploy/module4/requirements/browser.txt ./browser-requirements.txt
RUN python -m pip install --no-cache-dir --disable-pip-version-check -r browser-requirements.txt \
    && python -m pip check
COPY scripts/module4/module4d_browser_e2e.py ./module4d_browser_e2e.py
USER pwuser
ENTRYPOINT ["python", "module4d_browser_e2e.py"]

# Bounded Kubernetes API runner. Keep the existing spark/api/ui/browser
# targets and their defaults unchanged. This target carries only Module 4
# runtime sources, the Module 3 bundle contract imported by Module 4, the exact
# files hashed by src.module4.cli.source_digest(), and the API readiness probe.
FROM spark AS k8s-runner

ARG MODULE4_COMMIT
RUN set -eu; \
    case "$MODULE4_COMMIT" in \
      ''|*[!0-9a-f]*) echo 'MODULE4_COMMIT must be a full lowercase Git SHA' >&2; exit 1 ;; \
    esac; \
    if [ "${#MODULE4_COMMIT}" -ne 40 ]; then \
      echo 'MODULE4_COMMIT must be exactly 40 characters' >&2; exit 1; \
    fi; \
    groupadd --gid 10001 k8srunner; \
    useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin k8srunner
LABEL org.opencontainers.image.source="https://github.com/mihikari29/instacart-market-basket-analysis" \
      org.opencontainers.image.revision=${MODULE4_COMMIT}

WORKDIR /workspace
ENV PYTHONPATH=/workspace \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOME=/tmp \
    MODULE4D_APP_ROOT=/workspace \
    MODULE4_COMMIT=${MODULE4_COMMIT}

COPY src/__init__.py /workspace/src/__init__.py
COPY src/module3/__init__.py /workspace/src/module3/__init__.py
COPY src/module3/config.py /workspace/src/module3/config.py
COPY src/module3/trend_bundle.py /workspace/src/module3/trend_bundle.py
COPY src/module4/ /workspace/src/module4/

# These files are part of the established Module 4 provenance digest. Keep
# their paths and bytes intact even though the API does not execute the CLIs.
COPY scripts/module4.py /workspace/scripts/module4.py
COPY scripts/module4_runtime_smoke.py /workspace/scripts/module4_runtime_smoke.py
COPY scripts/module4_flow_smoke.py /workspace/scripts/module4_flow_smoke.py
COPY scripts/module4_cross_module_smoke.py /workspace/scripts/module4_cross_module_smoke.py
COPY Dockerfile /workspace/Dockerfile
COPY compose.yaml /workspace/compose.yaml
COPY requirements.txt /workspace/requirements.txt
COPY requirements-dev.txt /workspace/requirements-dev.txt

# Module 4D's registered historical evidence route reads exactly these files.
COPY docs/evidence/full/analytics.json /workspace/docs/evidence/full/analytics.json
COPY docs/evidence/full/current-hardening-2026-10-03.json /workspace/docs/evidence/full/current-hardening-2026-10-03.json
COPY docs/evidence/module4/full-ab-20261010-fixed-first/evaluation.json /workspace/docs/evidence/module4/full-ab-20261010-fixed-first/evaluation.json
COPY docs/evidence/module4c/c8a-receipt.json /workspace/docs/evidence/module4c/c8a-receipt.json
COPY docs/evidence/module4c/c8b-receipt.json /workspace/docs/evidence/module4c/c8b-receipt.json
COPY scripts/k8s/api_readiness.py /workspace/scripts/k8s/api_readiness.py

USER 10001:10001
EXPOSE 8000
CMD ["python3", "-c", "print('Kubernetes runner image; specify an explicit workload command')"]
