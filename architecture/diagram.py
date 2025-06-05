from diagrams import Cluster, Diagram
from diagrams.aws.network import ELB
from diagrams.onprem.database import Postgresql
from diagrams.onprem.queue import Rabbitmq
from diagrams.programming.language import Python

with Diagram("Taxi Pipeline", filename="architecture/architecture", show=False):
    with Cluster("Workers"):
        prod = Python("Producer")
        proc = Python("Processor")
        upl = Python("Uploader")
    mq = Rabbitmq("RabbitMQ")
    db = Postgresql("Postgres")
    api = Python("Presenter")
    ui = ELB("Streamlit")

    prod >> mq
    mq >> proc >> mq
    mq >> upl >> db >> api >> ui
