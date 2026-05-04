times=10
for test in 'TestTransferLeader3B'\
            'TestBasicConfChange3B'\
            'TestConfChangeRemoveLeader3B'\
            'TestConfChangeRecover3B'\
            'TestConfChangeRecoverManyClients3B'\
            'TestConfChangeUnreliable3B'\
            'TestConfChangeUnreliableRecover3B'\
            'TestConfChangeSnapshotUnreliableRecover3B'\
            'TestConfChangeSnapshotUnreliableRecoverConcurrentPartition3B'\
            # 'TestOneSplit3B'\
            # 'TestSplitRecover3B'\ 
            # 'TestSplitRecoverManyClients3B'\
            # 'TestSplitUnreliable3B'\
            # 'TestSplitUnreliableRecover3B'\
            # 'TestSplitConfChangeSnapshotUnreliableRecover3B'\
            # 'TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B';\
            do
    for ((i=1; i<=times; i++)); do
        rm -rf /tmp/*test-raftstore*
        echo "=== Running $test (attempt $i/${times}) ==="
        if ! go test ./kv/test_raftstore -run "^${test}" -count=1 -timeout=10m &> test.log; then
            echo "FAILED: $test on attempt $i"
            exit 1
        fi
    done
    echo "PASSED: $test (${times}/${times} attempts)"
done