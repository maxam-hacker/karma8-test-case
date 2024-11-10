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