| database   | Database Setup           |   CPU cores allocated | Main Memory usage   |
|:-----------|:-------------------------|----------------------:|:--------------------|
| postgres   | A single server          |                     8 | 7.654GiB            |
| citus      | A cluster of 3 nodes     |                     8 | 7.654GiB            |
| scylla     | A cluster of 3 nodes     |                     6 | 6GiB                |
| mongo      | A replica set of 3 nodes |                     8 | 7.654GiB            |

---
*Note: `ScyllaDB` was configured with 2 CPU shards and 2GB of memory, hence the total memory usage is 6GiB.*
**IMPORTANT:** *Total time metric might be inaccurate: it highly differs across multiple runs. I don't report average over multiple runs because it is too long.*
