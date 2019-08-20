from client import ClientConnectionPool
from protogen.company_pb2_grpc import CompanyServerStub
from manager import Manager

pool = ClientConnectionPool(stub_cls=CompanyServerStub)

manager = Manager(config="config.yaml")
print(manager.methods)
print(manager.pools)
from google.protobuf.empty_pb2 import Empty

for i in range(10):
    r = manager.GetAllCompany(Empty())
    # print(r)
