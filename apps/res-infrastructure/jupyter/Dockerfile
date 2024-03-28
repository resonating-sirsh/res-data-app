ARG RES_ENV
ARG RES_TAG
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:${RES_TAG:-latest}

WORKDIR /app

ADD ./docs/notebooks /notebooks
ADD ./res-schemas/pydantic/ /app/schemas/pydantic/

RUN pip install jupyter

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH "${PYTHONPATH}:/app"

EXPOSE 5000
CMD ["jupyter", "notebook", "--port=5000", "--no-browser", "--ip=0.0.0.0", "--allow-root", "--NotebookApp.token=''", "--notebook-dir=/notebooks"]

#--nudge