ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:latest

RUN pip uninstall -y torch
RUN pip install torch

ENTRYPOINT ["python", "-m", "res.learn.optimization.packing.annealed.nn"]