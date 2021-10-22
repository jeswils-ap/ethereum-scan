# ethereum-scan

This application crawls Ethereum transactions, given an address (wallet) and starting block number, and shows change in value over time.

## Requires

requests
pandas
kaleido
flask

## Usage

```bash
python ethereum_test.py
```

Simply run the script, open a browser and navigate to localhost:5001. There you will input address and starting block. After clicking run, the page will display a table of all transactions and a graph showing value and change in value over time.
