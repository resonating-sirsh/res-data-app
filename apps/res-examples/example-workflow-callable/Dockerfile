FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-media-base:latest
WORKDIR /app/
COPY requirements.txt /app/res/
RUN pip install -U pip && pip install --no-cache-dir -r /app/res/requirements.txt
COPY res /app/res/
ENV PYTHONPATH "${PYTHONPATH}:/app"
ENV PYTHONUNBUFFERED=0
#...
ENTRYPOINT ["python", "-m", "res",  "flow", "run", "flow_node_example"]
