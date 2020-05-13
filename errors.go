package mydb

const (
	noReadReplicaError      = "Provide at least one read replica"
	replicaPingFailError    = "replica db %d ping fail: %s"
	masterPingFailError     = "master's db ping fail: %s"
	pingChannelCloseError   = "Ping Channel is closed"
	noReplicaAvailableError = "No replica is alive for reading data"
)
