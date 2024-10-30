module karma8-storage/tester

go 1.21

replace (
	karma8-storage/api => ../../api
	karma8-storage/internals => ../../internals
)

require (
	karma8-storage/api v1.0.0
	karma8-storage/internals v1.0.0
)