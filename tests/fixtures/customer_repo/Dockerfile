FROM public.ecr.aws/resim/open-builds/field-sessions@sha256:68029e0a856cd5aa23471d2aa74f1717b4eb5dc0c9795a0c2444f3a98c78a7c2 AS job
WORKDIR /app
COPY job.py config.resim.yml ./
ENTRYPOINT ["python", "/app/job.py"]

FROM job AS test
ENV SESSION_ROBOT_PROFILE=burro
COPY tests/make_fixture.py tests/check_emissions.py tests/run_checks.py /checks/
COPY Dockerfile /checks/Dockerfile
RUN python /checks/run_checks.py

FROM scratch AS test-results
COPY --from=test /test-results/ /

FROM job AS runtime
