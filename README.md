# ETH Transactions 

## Datalake:

### Batch:
```
aws s3 cp "s3://aws-public-blockchain/v1.0/eth/transactions/date=2023-11-01/part-00000-805de656-8631-4f64-83b6-62610ff9d4ca-c000.snappy.parquet" . --profile my-dev-profile
```

<details>
<summary> Table Data </summary>

| Field                       | Type      | Description                                                                  |
|-----------------------------|-----------|------------------------------------------------------------------------------|
| date                        | string    | Partition column (YYYY-MM-DD)                                                |
| hash                        | string    | Hash of the transaction                                                      |
| nonce                       | bigint    | The number of transactions made by the sender prior to this one              |
| transaction_index           | bigint    | Integer of the transactions index position in the block                      |
| from_address                | string    | Address of the sender                                                        |
| to_address                  | string    | Address of the receiver                                                      |
| value                       | double    | Value transferred in wei                                                     |
| gas                         | bigint    | Gas price provided by the sender in wei                                      |
| gas_price                   | bigint    | Gas provided by the sender                                                   |
| input                       | string    | The data sent along with the transaction                                     |
| receipt_cumulative_gas_used | bigint    | The total amount of gas used when this transaction was executed in the block |
| receipt_gas_used            | bigint    | The amount of gas used by this specific transaction alone                    |
| receipt_contract_address    | string    | The contract address created, if the transaction was a contract creation     |
| receipt_status              | bigint    | If the transaction was successful                                            |
| block_timestamp             | timestamp | The unix timestamp for when the block was collated                           |
| block_number                | bigint    | Block number where this transaction was in                                   |
| block_hash                  | string    | Hash of the block                                                            |
| max_fee_per_gas             | bigint    | Total fee that covers both base and priority fees                            |
| max_priority_fee_per_gas    | bigint    | Fee given to miners to incentivize them to include the transaction           |
| transaction_type            | bigint    | Transaction type                                                             |
| receipt_effective_gas_price | bigint    | The actual value per gas deducted from the sender's account                  |

</details>

### Streaming:
```text
https://bitquery.io/
```

## Technologies:
- Python
- Docker
- Apache Hadoop
- Apache Spark
- Apache Airflow
- MongoDB
- Metabase
- Kafka
- Zookeeper
  





