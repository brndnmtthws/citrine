# Start epmd
:os.cmd('epmd -daemon')

# start the current node as a manager
:ok = LocalCluster.start()

# run all tests!
ExUnit.start(capture_log: true)
