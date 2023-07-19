# dp-starj
The file structure is as below
```
project
│   
└───code
└───data
│   └───Graph
│   └───SSB
└───query
└───techrep
```
`./code` stores the codes.

`./data` stores graph and SSB datasets.

`./query` stores the queries used in the experiments of the paper.

`./techrep` stores the technical report of the paper.


## Prerequisites
### Tools
Before running this project, please install below tools
* [PostgreSQL](https://www.postgresql.org/)
* [Python3](https://www.python.org/download/releases/3.0/)

### Python Dependency
Here are dependencies used in python programs:
* `time`
* `numpy`
* `psycopg2`

### Download Data
Download two data packages ([Graph] and [SSB]) in `./data`.
The fact table of SSB is in the `master` branch.


### Create PostgreSQL Database
To create an empty PostgreSQL database, for example, named "Deezer", run
```
createdb Deezer;
```

Here, we need two databases for the graph dataset: `Deezer` and `Amazon`. For SSB dataset, we use 2 different datasets: `ssb`  and `ssb1` which are raw data and different distribution data.


## Running
For the SSB queries, run the `q*` in the code. Run the `k*` in the code for the k-star queries. `distributions` is to generate different data distributions to update the database and run the `workload` for the star-join workload queries. Running the R2T, LS, and TM, please refer to [Comparison schemes](https://github.com/hkustDB/Race-to-the-Top).
