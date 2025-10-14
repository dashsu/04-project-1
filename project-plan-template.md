# Project plan

## Objective

The objective of this project is to build an ETL pipeline to extract stock market data from the MarketStack API, transform it into a clean and structured format, and load it into a PostgreSQL database.

## Consumers

Data Analysts: Require clean and structured datasets for analysis of stock trades, market trends and help in decision making.

## Questions

> - How many trades occur for each stock symbol per day?
> - How many trades were executed on a particular date or in a specific date range?
> - Which symbols have the highest trading volume?
## Source datasets

| Source name | Source type | Source documentation |
| - | - | - |
| Trade database | PostgreSQL database | - |
| MarketStack API | REST API | - |

## Solution architecture

- Data extraction
    Extract data from Marketstack API using the python ETL pipeline.
    Incremental extraction based on latest data pulled from the source.
- Data transformation
    Clean missing and incorrect data.
    Standardize date formats.
- Data Loading
    Load transformed data into PostgreSQL using SQLAlchemy.
    Do a full load if target table is empty, incremental otherwise.
