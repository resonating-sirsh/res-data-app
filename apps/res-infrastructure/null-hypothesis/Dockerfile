ARG RES_ENV
ARG RES_TAG
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:${RES_TAG:-latest}

ENV RES_TAG="${RES_TAG:-latest}"
RUN pip install \
    "fastapi>=0.85.0,<1" \
    "pydantic>=1.10.2,<2" \
    "hypercorn" \
    "uvicorn" \
    "stringcase"

ADD ./apps/res-infrastructure/null-hypothesis /app/
ADD ./res-schemas/pydantic/ /app/schemas/pydantic/

ENV PYTHONPATH "${PYTHONPATH}:/app/"
EXPOSE 5000
# CMD ["uvicorn", "--host", "0.0.0.0", "--port", "5000", "src.asgi:app"]
CMD ["hypercorn", "--bind", "0.0.0.0:5000", "src.asgi:app"]
