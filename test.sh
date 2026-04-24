times=10
for test in TestConfChangeUnreliable3B; do
    for ((i=1; i<=times; i++)); do
        rm -rf /tmp/*test-raftstore*
        echo "=== Running $test (attempt $i/${times}) ==="
        if ! go test ./kv/test_raftstore -run "^${test}" -count=1 -timeout=3m &> test.log; then
            echo "FAILED: $test on attempt $i"
            exit 1
        fi
    done
    echo "PASSED: $test (${times}/${times} attempts)"
done