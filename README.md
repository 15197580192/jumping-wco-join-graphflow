# jumping-wco-join-graphflow
--------------------

Table of Contents
-----------------
  * [Overview](#Overview)
  * [Build Steps](#Build-Steps)
  * [Executing Queries](#Executing-Queries)
  * [Contact](#contact)

Overview
-----------------
This is the code of the paper:Variable-length Path Query Evaluation based on Worst-Case Optimal Joins.Here we present how we embed our jumping-like worst-case optimal join technique in [graphflow-optimizer](https://github.com/queryproc/optimizing-subgraph-queries-combining-binary-and-worst-case-optimal-joins?tab=readme-ov-file).

Build Steps
-----------------
* To do a full clean build: `./gradlew clean build installDist`
* All subsequent builds: `./gradlew build installDist`

Executing Queries
-----------------
### Getting Started
After building Graphflow, run the following command in the project root directory:
```
. ./env.sh
```
You can now move into the scripts folder to load a dataset and execute queries:
```
cd scripts
```

### Dataset Preperation
A dataset may consist of two files: (i) a vertex file, where IDs are from 0 to N and each line is of the format (ID,LABEL); and (ii) an edge file where each line is of the format (FROM,TO,LABEL). If the vertex file is omitted, all vertices are assigned the same label. We mainly used datasets from [SNAP](https://snap.stanford.edu/). The `serialize_dataset.py` script lets you load datasets from csv files and serialize them to Graphflow format for quick subsequent loading.

To load and serialize a dataset from a single edges files, run the following command in the `scripts` folder:
```
python3 serialize_dataset.py /absolute/path/edges.csv /absolute/path/data
```
The system will assume that all vertices have the same label in this case. The serialized graph will be stored in the `data` directory. If the dataset consists of an edges file and a vertices file, the following command can be used instead:
```
python3 serialize_dataset.py /absolute/path/edges.csv /absolute/path/data -v /absolute/path/vertices.csv
```
After running one of the commands above, a catalog can be generated for the optimizer using the `serialize_catalog.py` script.
```
python3 serialize_catalog.py /absolute/path/data  
```

### Executing Queries
Once a dataset has been prepared, executing a query is as follows:
```
python3 execute_query.py "(a)->(b),(b)->(c),(c)->(d)" /absolute/path/data
```
## example
```
cd jumping-wco-join-graphflow
python3 scripts/serialize_dataset.py test/web-Google.txt ./data
python3 scripts/serialize_catalog.py ./data
# Execute a query with length of 5 using our jumping-like-wco join technique
python3 scripts/execute_query.py "(p1)-[3]->(p2),(p2)-[3]->(p3),(p3)-[3]->(p4),(p4)-[3]->(p5),(p5)-[3]->(p6)" ./data 
```



