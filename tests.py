from client import ClientConnectionPool
from protogen.company_pb2_grpc import CompanyServerStub
from manager import Manager

pool = ClientConnectionPool(stub_cls=CompanyServerStub)

manager = Manager()
manager.register(pool)

from google.protobuf.empty_pb2 import Empty

r = manager.GetAllCompany(Empty())
print(r)
