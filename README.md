# readnetfs (aka chrislfs)
```
Usage of ./readnetfs:
  -bind string
        Bind address and port in x.x.x.x:port format
  -mnt string
        Directory to mount the net filesystem on
  -peer value
        Peer addresses and ports in x.x.x.x:port format, has to specified for each peer like so: -peer x.x.x.x:port -peer x.x.x.x:port ...
  -rate int
        rate limit in Mbit/s (default 1000)
  -receive
        Receive files and mount the net filesystem on the mnt directory
  -send
        Serve files from the src directory
  -src string
        Directory to serve files from
```
