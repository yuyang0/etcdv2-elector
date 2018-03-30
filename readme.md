# Etcdv2 elector
## usage

	e1, err = New(etcds, "/tmp/aaa/election", "127.0.0.1:1111", TTL)
	if err != nil {
		panic(err)
	}
	ch1, err := e1.Run()
	if err != nil {
		panic(err)
	}
    ret := <-ch1
    if ret {
        // become leader
    }
