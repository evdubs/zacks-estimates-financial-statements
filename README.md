# zacks-estimates-financial-statements

These Racket programs will download the Zacks "Detailed Estimates" and "Financials" HTML documents and insert the 
estimates/statement data into a PostgreSQL database. The intended usage is:

```bash
$ Racket estimate-extract.rkt
$ Racket estimate-transform-load.rkt
```

```bash
$ Racket financial-statement-extract.rkt
$ Racket balance-sheet-transform-load.rkt
$ Racket cash-flow-statement-transform-load.rkt
$ Racket income-statement-transform-load.rkt
```

The provided schema.sql file shows the expected schema within the target PostgreSQL instance. 
This process assumes you can write to a /var/tmp/zacks folder. This process also assumes you have loaded your database with NASDAQ symbol
file information. This data is provided by the [nasdaq-symbols](https://github.com/evdubs/nasdaq-symbols) project.

The above process will download around 2GB worth of HTML documents over many hours. It is encouraged to compress these files when you are 
done processing them. It is also encouraged that you do not run the extract jobs too frequently. I think running the estimate-extract 
once per week and the financial-statement-extract once per month is sufficient.
