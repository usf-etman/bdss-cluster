# Big Data, Small Steps
This is Big Data, Small Steps!

My humble side hustle to train and teach myself & others who are passionate about big data and data engineering as much as I am.

## Big Data Cluster
This repo represents a Dockerized Big data cluster that I built for training/teaching purposes, It has:
1. **Hadoop** configured with high availability for HDFS & YARN.
2. **Hive** running on Tez.
3. **Spark** running on YARN & Hive metastore.
4. **Kafka** running on single node which I find suitable for our learning cluster.
5. **ZooKeeper** For its ZAB consensus algorithm to help Hadoop in electing its active Namenode/ResourceManager.
6. **Postgres** Acting as a metdata db for hive.
7. **Iceberg** For ACID transaction tables on Hadoop.
8. **DataFusion Comet** An accelerator for Spark engine.
9. **Jupyter Notebook** For interactive development.

### Architecture
The below photo shows the cluster containers and how they interact with each other.
- Each circle is a container and you can see what services are run on each.
- Note: TEZ, SPARK, ICEBERG & COMET are binaries not services.
<img width="745" height="877" alt="Our labs environment" src="https://github.com/user-attachments/assets/36d08904-6d9d-4e42-b2aa-00738aff740a" />

### Stack versions
| Component   | Version |
|-------------|----------|
| Java        | 17       |
| Python      | 3.12     |
| Hadoop      | 3.4.2    |
| ZooKeeper   | 3.8.4    |
| Hive        | 4.1.0    |
| Tez         | 0.10.5   |
| PostgreSQL  | 15       |
| Spark       | 4.1.1    |
| Kafka       | 4.2.0    |
| Iceberg     | 1.10.1   |
| DataFusion Comet | 0.14.0 |

## Laptop Prerequisites
Below are the minimum resources needed:
- Memory: 8GB RAM
- CPU: 4 Physical Cores / 8 Logical Cores
- Storage: 10GB are needed to download Docker images + data, Make sure you have more than 10% free disk space after installation for the cluster to work properly.
- Make sure to setup Docker Desktop

Preferred resources:
- Memory: 12GB+ RAM
- CPU: 6 Physical Cores / 12 Logical Cores

## Usage
1. To start the cluster, first you need to clone the repo:
```
git clone https://github.com/usf-etman/bdss-cluster.git
```

2. Second, move into "bdss-cluster" and run compose (this might take some time):
```
cd bdss-cluster
docker compose up
```

## Web Interfaces
Below is a list of all available UIs that you can use to test that everything is up & running.
| Component                | URL |
|--------------------------|-----|
| HDFS NameNode (master1) | [http://localhost:19870](http://localhost:19870) |
| HDFS NameNode (master2) | [http://localhost:29870](http://localhost:29870) |
| YARN ResourceManager (master1) | [http://localhost:18088](http://localhost:18088) |
| YARN ResourceManager (master2) | [http://localhost:28088](http://localhost:28088) |
| JournalNode 1 | [http://localhost:18480](http://localhost:18480) |
| JournalNode 2 | [http://localhost:28480](http://localhost:28480) |
| JournalNode 3 | [http://localhost:38480](http://localhost:38480) |
| HiveServer2 | [http://localhost:10002](http://localhost:10002) |
| Spark UI | [http://localhost:18080](http://localhost:18080) |
| Jupyter Notebook | [http://localhost:8000](http://localhost:8000) |

