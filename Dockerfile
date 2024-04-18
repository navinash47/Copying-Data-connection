FROM aus-harboreg-01.bmc.com/helix-gpt/main/base-image-helixgpt:latest as production

USER 1000

WORKDIR /opt/bmc/data-connection

COPY --chown=bmcuser:bmc ./requirements.txt /opt/bmc/data-connection

RUN pip install --no-cache-dir -r requirements.txt;

COPY --chown=bmcuser:bmc ./src /opt/bmc/data-connection/app

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--app-dir", "app"]

FROM production as test

COPY --chown=bmcuser:bmc ./tests /opt/bmc/data-connection/tests

RUN PYTHONPATH=./app pytest --cov=app --cov-branch --cov-report=xml --junitxml=test-results.xml tests

RUN sed -i 's|<source>.*</source>|<source>/usr/src/src</source>|' coverage.xml

FROM scratch AS export-stage

COPY --from=test /opt/bmc/data-connection/coverage.xml /

COPY --from=test /opt/bmc/data-connection/test-results.xml /
