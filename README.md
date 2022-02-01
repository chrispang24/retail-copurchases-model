# Retail Baseline Recommendation Model

## Overview

Goal is to generate a baseline model for product recommendations based solely on historical item co-purchases.

**RecommenderBuilder** - computes recommendation results and persists them to a file store.

**RecommenderServer** - reads results into memory for fast retrieval upon input prompt.

![Recommender Diagram](docs/diagram.png)

## Source Data Files

Data files (i.e. products.txt, transactions.txt) should be stored in 'data' folder for correct execution. Samples files are provided.

- products.txt - tab separated list (product ID, category code, product name)
- transactions.txt - json list of customer transactions

## Installation

#### Create and enable virtual environment

```
python -m venv venv/
source venv/bin/activate
```

#### Install required dependencies

```
pip install -r requirements.txt
```

## Usage

Within virtual environment

#### Serve recommender model results

```
python recommender.py
```

#### Build model results prior to serving

```
python recommender.py build
```

## Folder Structure

- data: raw source files
- docs: summary write-up
- output: processed output files
- recommender: source files for recommendation model building and serving
- submission: source file used for test submission
