from diagrams import Cluster, Diagram
from diagrams.aws.compute import EC2
from diagrams.aws.database import RDS
from diagrams.aws.network import ELB
from diagrams.custom import Custom
from diagrams.onprem.database import MSSQL
from diagrams.onprem.database import MongoDB
from diagrams.elastic.elasticsearch import Elasticsearch
from diagrams.onprem.queue import Kafka
from diagrams.azure.database import CosmosDb
from diagrams.programming.language import Csharp

graph_attr = {
    "fontsize": "12",
    "labeldistance": "10",
    "labelfloat": "true",
    # "compund": "True"
}

node_attr = {
    "height": "10.6",
    "fontsize": "12"
}


with Diagram("Flowtide streaming integration", show=False, direction="TB", graph_attr=graph_attr, node_attr=node_attr):

    with Cluster("Join data from different sources and react on changes"):
        flowtide = Custom("Flowtide", "../logo/flowtidelogo_256x256.png", labelloc="b")
        MongoDB("MongoDB", labelloc="t") >> flowtide
        MSSQL("SQL Server", labelloc="t") >> flowtide >> Elasticsearch("Elasticsearch")
        Csharp("Custom source", labelloc="t") >> flowtide

    with Cluster("send data and changes to one or multiple destinations"):
        flowtide = Custom("Flowtide", "../logo/flowtidelogo_256x256.png", labelloc="b")
        MSSQL("SQL Server", labelloc="t") >> flowtide >> MSSQL("SQL Server", labelloc="b")
        flowtide >> Elasticsearch("Elasticsearch")
        flowtide >> MongoDB("MongoDB")
        Kafka("kafka", labelloc="t") >> flowtide
        flowtide >> CosmosDb("CosmosDB")
        flowtide >> Csharp("Custom sink")