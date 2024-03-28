import ray
import res

s3 = res.connectors.load("s3")


@ray.remote
def hello_world():
    print("hello world")
    return "hello world"


print("create ray")
a = ray.init(address="ray://localhost:10001")

print(a)

print("test a thing")


@ray.remote
def f(x):
    return dict(s3.read("s3://res-data-platform/samples/acq.parquet").iloc[0])


futures = [f.remote(i) for i in range(4)]
print(ray.get(futures))  # [0, 1, 4, 9]


# helm install raycluster kuberay/ray-cluster --set image.tag=latest --set image.repository=286292902993.dkr.ecr.us-east-1.amazonaws.com/res-ray --set replicas=5
