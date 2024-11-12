### Start up the system...
```
cd ./docker && docker compose build && docker compose up
```

### Build tester and process testing...
```
cd ./tester && go build
```

```
./tester -operation=upload -bucket=objectBucketName -key=objectKeyValue -target=path/to/uploaded/file
```

```
./tester -operation=download -bucket=objectBucketName -key=objectKeyValue -target=path/to/stored/file
```

```
diff path/to/uploaded/file path/to/stored/file
```

### or use swagger interface for Ingestor service

After the system started up it is availabale swagger page for Ingerstor service on your local machine.

By default:

```
    http://127.0.0.1:7788/swagger/index.html
```
