module karma8-storage/shard-manager

go 1.21

replace (
	karma8-storage/api => ../api
	karma8-storage/internals => ../internals
)

require (
	github.com/go-chi/chi v1.5.5
	github.com/stretchr/testify v1.9.0
	karma8-storage/api v1.0.0
	karma8-storage/internals v1.0.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
