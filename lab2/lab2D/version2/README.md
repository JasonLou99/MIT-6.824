update `Reply.Term` in InstallSnapshot() when `args.Term <= rf.currentTerm`

update `Reply` in AppendEntries() when `args.PrevLogIndex < rf.log[0].Index`

`bash go-test-many.sh 100 4`: `0 failed`

`bash go-test-many.sh 60 4 TestSnapshotInstallUnreliable2D`: `3 failed`
