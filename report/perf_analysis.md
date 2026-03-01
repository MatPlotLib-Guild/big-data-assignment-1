
| Database | Q1       | Q2      | Q3       | Q4     | Database Setup           | CPU cores allocated | Main Memory usage |
| -------- | -------- | ------- | -------- | ------ | ------------------------ | ------------------- | ----------------- |
| citus    | 31.295s  | 46.467s | 57.013s  | 0.300s | A cluster of 3 nodes     | 8                   | 7.654GiB          |
| mongo    | 266.851s | 0.205s  | 121.051s | 1.085s | A replica set of 3 nodes | 8                   | 7.654GiB          |
| postgres | 4.436s   | 5.417s  | 62.556s  | 0.099s | A single server          | 8                   | 7.654GiB          |
| scylla   | 0.004s   | 2.392s  | 0.002s   | 0.054s | A cluster of 3 nodes     | 6                   | 6GiB              |


