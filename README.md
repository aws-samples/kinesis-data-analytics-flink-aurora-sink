<h3 align="center">Kinesis Data Analytics for Apache Flink Aurora Sink Postgres Example (with POJO)</h3>

<div align="center">


</div>


--------
>  #### 🚨 August 30, 2023: Amazon Kinesis Data Analytics has been renamed to [Amazon Managed Service for Apache Flink](https://aws.amazon.com/managed-service-apache-flink).

--------


<p>This Kinesis Data Analytics for Apache Flink Application reads from a Kinesis Data Stream, Serializes the records and then writes them to an Aurora Postgres Table (every 100 messages).
    <br> 
</p>

## 📝 Table of Contents

- [📝 Table of Contents](#-table-of-contents)
- [🏁 Getting Started <a name = "getting_started"></a>](#-getting-started-)
- [Prerequisites](#prerequisites)
- [Installing](#installing)


## 🏁 Getting Started <a name = "getting_started"></a>

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See [deployment](#deployment) for notes on how to deploy the project on Kinesis Data Analytics for Apache Flink.

### Prerequisites

```
- Java 1.8
- Apache Flink
- A Local or Remote Postgres Database
- A Kinesis Data Stream
```

### Installing


```
Load the project into an IDE as a Maven Project and define all of the necessary environment variables in `config.json` specific to your application's needs.

```

When the application runs, it will read data from Kinesis Data Analytics, deserialize into a POJO and load it into Postgres.


### Note:
Ensure you are running with the correct connectivity and permissions--the application must be run in the same VPC as the Postgres DB or have access to it.


# License
This library is licensed under the MIT-0 License. See the LICENSE file.