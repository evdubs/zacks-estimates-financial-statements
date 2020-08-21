# zacks-estimates-financial-statements

These Racket programs will download the Zacks "Detailed Estimates" and "Financials" HTML documents and insert the 
estimates/statement data into a PostgreSQL database. The intended usage is:

```bash
$ racket estimate-extract.rkt
$ racket estimate-transform-load.rkt
```

```bash
$ racket financial-statement-extract.rkt
$ racket balance-sheet-transform-load.rkt
$ racket cash-flow-statement-transform-load.rkt
$ racket income-statement-transform-load.rkt
```

```bash
$ racket earnings-calendar-extract.rkt
$ racket earnings-calendar-transform-load.rkt
```

You will need to provide a password for many of the above programs. The available parameters are:

```bash
$ racket estimate-extract.rkt -h
racket estimate-extract.rkt [ <option> ... ]
 where <option> is one of
  -f <first>, --first-symbol <first> : First symbol to query. Defaults to nothing
  -l <last>, --last-symbol <last> : Last symbol to query. Defaults to nothing
  -n <name>, --db-name <name> : Database name. Defaults to 'local'
  -p <password>, --db-pass <password> : Database password
  -u <user>, --db-user <user> : Database user name. Defaults to 'user'
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-'; for
  example: `-h-' is the same as `-h --'

$ racket estimate-transform-load.rkt -h
racket estimate-transform-load.rkt [ <option> ... ]
 where <option> is one of
  -b <folder>, --base-folder <folder> : Zacks estimates base folder. Defaults to /var/tmp/zacks/estimates
  -d <date>, --folder-date <date> : Zacks estimates folder date. Defaults to today
  -n <name>, --db-name <name> : Database name. Defaults to 'local'
  -p <password>, --db-pass <password> : Database password
  -u <user>, --db-user <user> : Database user name. Defaults to 'user'
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-'; for
  example: `-h-' is the same as `-h --'

$ racket financial-statement-extract.rkt -h
racket financial-statement-extract.rkt [ <option> ... ]
 where <option> is one of
  -f <first>, --first-symbol <first> : First symbol to query. Defaults to nothing
  -l <last>, --last-symbol <last> : Last symbol to query. Defaults to nothing
  -n <name>, --db-name <name> : Database name. Defaults to 'local'
  -p <password>, --db-pass <password> : Database password
  -u <user>, --db-user <user> : Database user name. Defaults to 'user'
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-'; for
  example: `-h-' is the same as `-h --'

$ racket balance-sheet-transform-load.rkt -h
racket balance-sheet-transform-load.rkt [ <option> ... ]
 where <option> is one of
  -b <folder>, --base-folder <folder> : Zacks balance sheet base folder. Defaults to /var/tmp/zacks/balance-sheet
  -d <date>, --folder-date <date> : Zacks balance sheet folder date. Defaults to today
  -n <name>, --db-name <name> : Database name. Defaults to 'local'
  -p <password>, --db-pass <password> : Database password
  -u <user>, --db-user <user> : Database user name. Defaults to 'user'
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-'; for
  example: `-h-' is the same as `-h --'

$ racket cash-flow-statement-transform-load.rkt -h
racket cash-flow-statement-transform-load.rkt [ <option> ... ]
 where <option> is one of
  -b <folder>, --base-folder <folder> : Zacks cash flow statement base folder. Defaults to /var/tmp/zacks/cash-flow-statement
  -d <date>, --folder-date <date> : Zacks cash flow statement folder date. Defaults to today
  -n <name>, --db-name <name> : Database name. Defaults to 'local'
  -p <password>, --db-pass <password> : Database password
  -u <user>, --db-user <user> : Database user name. Defaults to 'user'
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-'; for
  example: `-h-' is the same as `-h --'

$ racket income-statement-transform-load.rkt -h
racket income-statement-transform-load.rkt [ <option> ... ]
 where <option> is one of
  -b <folder>, --base-folder <folder> : Zacks income statement base folder. Defaults to /var/tmp/zacks/income-statement
  -d <date>, --folder-date <date> : Zacks income statement folder date. Defaults to today
  -n <name>, --db-name <name> : Database name. Defaults to 'local'
  -p <password>, --db-pass <password> : Database password
  -u <user>, --db-user <user> : Database user name. Defaults to 'user'
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-'; for
  example: `-h-' is the same as `-h --'

$ racket earnings-calendar-extract.rkt -h
racket earnings-calendar-extract.rkt [ <option> ... ]
 where <option> is one of
  -e <ed>, --end-date <ed> : End date. Defaults to today + 6 weeks
  -s <sd>, --start-date <sd> : Start date. Defaults to today
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-'; for
  example: `-h-' is the same as `-h --'

$ racket earnings-calendar-transform-load.rkt -h
racket transform-load.rkt [ <option> ... ]
 where <option> is one of
  -b <folder>, --base-folder <folder> : Earnings Calendar base folder. Defaults to /var/tmp/zacks/earnings-calendar
  -d <date>, --folder-date <date> : Earnings Calendar folder date. Defaults to today
  -n <name>, --db-name <name> : Database name. Defaults to 'local'
  -p <password>, --db-pass <password> : Database password
  -u <user>, --db-user <user> : Database user name. Defaults to 'user'
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-'; for
  example: `-h-' is the same as `-h --'
```

The provided `schema.sql` file shows the expected schema within the target PostgreSQL instance. 
This process assumes you can write to a `/var/tmp/zacks` folder. This process also assumes you have loaded your database with NASDAQ symbol
file information. This data is provided by the [nasdaq-symbols](https://github.com/evdubs/nasdaq-symbols) project.

The above process will download around 2GB worth of HTML documents over many hours. It is encouraged to compress these files when you are 
done processing them. It is also encouraged that you do not run the extract jobs too frequently. I think running the estimate-extract 
once per week and the financial-statement-extract once per month is sufficient.

### Dependencies

It is recommended that you start with the standard Racket distribution. With that, you will need to install the following packages:

```bash
$ raco pkg install --skip-installed gregor html-parsing sxml tasks threading
```
